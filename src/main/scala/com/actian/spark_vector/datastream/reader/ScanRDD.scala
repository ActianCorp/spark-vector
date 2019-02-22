/*
 * Copyright 2016 Actian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.actian.spark_vector.datastream.reader

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls

import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ SparkListener, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.CollectionAccumulator

import com.actian.spark_vector.datastream.{ DataStreamClient, VectorEndpointConf }
import com.actian.spark_vector.util.ResourceUtil._
import com.actian.spark_vector.vector.{ ColumnMetadata, VectorConnectionProperties }

/**
 * `Vector` RDD to load data into `Spark` through `Vector`'s `Datastream API`
 */
class ScanRDD(@transient private val sc: SparkContext, 
              val connectionProps: VectorConnectionProperties, 
              val table: String, 
              val tableColumnMetadata: Seq[ColumnMetadata], 
              val selectQuery: String, 
              val whereParams: Seq[Any] = Nil, 
              private var vpartitions: Int = 0) 
              extends RDD[InternalRow](sc, Nil) {
  
  private val accumulator = sc.collectionAccumulator[Int]
  
  /** Custom scanner that contains specific connection info **/ 
  @volatile private var scanner: Scanner = _
  
  /** Custom spark listener to control setup  */
  @transient private val vectorJobqueue = new VectorJobManager()
  VectorJobManager.register(sc, vectorJobqueue);
    
  protected class VectorJobManager() extends SparkListener with Serializable { 
    import scala.collection.mutable.Queue
    import scala.collection.mutable.HashMap
    
    private val partList = Set.empty[Int]
    private val jobqueue = Queue.empty[Int]
    private val jobrefs = new HashMap[Int, (Future[Int], DataStreamClient, Scanner)]
    private var client: DataStreamClient = _
    private var partAccum: CollectionAccumulator[Int] = _
    
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      if (jobStart.stageInfos.exists(_.rddInfos.exists(_.id == ScanRDD.this.id))) {
        jobqueue.enqueue(jobStart.jobId)
      }
    }
    
    def getClient(): DataStreamClient = synchronized ({
      if (client == null) {
        client = new DataStreamClient(connectionProps, table)
      }
      client
    })
    
    def setAccumulator(accumulator: CollectionAccumulator[Int]) = {
      partAccum = accumulator
    }
    
    def readyJob(readConf: VectorEndpointConf): Unit = synchronized ({
      // Need to wait for job to start
      while (jobqueue.size == 0) {
        Thread.sleep(1)
      }
      
      val jobId = jobqueue.dequeue()
      closeResourceOnFailure(getClient) {
        val unloadOp = client.startUnload(selectQuery, whereParams)
        scanner = new Scanner(sc, readConf, new DataStreamReader(readConf, tableColumnMetadata))
        jobrefs.put(jobId, (unloadOp, client, scanner))
        logDebug("Ready to unload Vector")
      }
      // clear client reference
      client = null
    })
    
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = if (jobrefs.contains(jobEnd.jobId)) {
      logDebug(s"Job ${jobEnd.jobId} cleanup")
      val (unloadOperation, client, scan) = jobrefs.remove(jobEnd.jobId).get
      try {
        Await.result(unloadOperation, Duration.create(100, "ms"))
      } catch {
        case e: Exception => {
          logDebug("Client could not cleanly disconnect from Vector, manually closing datastreams")
          scan.touchDatastreams(partAccum.value.asScala.toList)
        }
      } finally {
        scan.closeAll()
        client.close()
        logDebug(s"Unload vector job ${jobEnd.jobId} ended @ ${jobEnd.time}.")
        partAccum.reset()
      }
    }
    
  }
  
  object VectorJobManager {
    def register(sc: SparkContext, listener: VectorJobManager): Unit = sc.addSparkListener(listener)
    def deregister(sc: SparkContext, listener: VectorJobManager): Unit = sc.removeSparkListener(listener)
  }
  
  private var _readConf: VectorEndpointConf = VectorEndpointConf(IndexedSeq())
  protected def readConf = _readConf
  protected def readConf_= (value: VectorEndpointConf):Unit = _readConf = value
  
  private def initializeReadconf(client: DataStreamClient): VectorEndpointConf = {
    client.prepareUnloadDataStreams(vpartitions)
    val conf =  client.getVectorEndpointConf
    logDebug(s"Configured ${conf.size} Vector endpoints for unloading")
    conf
  }

  override protected def getPartitions: Array[Partition] = {
    if (vpartitions == 0)
      vpartitions = 8;
    (0 until vpartitions).map(idx => new Partition { def index = idx }).toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    if (split.index == 0) {
      // Need to ensure partitions are initialized
      // Only need to setup once per read for all partitions
      readConf = initializeReadconf(vectorJobqueue.getClient())
      vectorJobqueue.setAccumulator(accumulator)
      vectorJobqueue.readyJob(readConf)
    }
    Seq(readConf.vectorEndpoints(split.index).host)
  }
  
  override def compute(split: Partition, taskContext: TaskContext): Iterator[InternalRow] = {
    accumulator.add(split.index)
    scanner.compute(split, taskContext)
  }

}
