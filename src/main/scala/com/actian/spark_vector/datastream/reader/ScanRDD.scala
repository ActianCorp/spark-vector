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

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls

import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ SparkListener, SparkListenerJobEnd, SparkListenerJobStart }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow

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
  
  /** Closed states for the datastream connection */
  @volatile private var closed = true
  
  /** Custom row iterator for reading `DataStream`s in row format */
  @volatile private var it: RowReader = _
  
  /** Custom spark listener to control setup  */
  @transient private val vectorJobqueue = new SparkListener() {
    import scala.collection.mutable.Queue
    import scala.collection.mutable.HashMap
    
    private val jobqueue = Queue.empty[Int]
    private val jobrefs = new HashMap[Int, (Future[Int], DataStreamClient, DataStreamReader)]
    
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      if (jobStart.stageInfos.exists(_.rddInfos.exists(_.id == ScanRDD.this.id))) {
        jobqueue.enqueue(jobStart.jobId)
      }
    }
    
    def readyJob(): Unit = synchronized ({
      // Need to wait for job to start
      while (jobqueue.size == 0) {
        Thread.sleep(1)
      }
      
      val jobId = jobqueue.dequeue()
      val client = new DataStreamClient(connectionProps, table)
      closeResourceOnFailure(client) {
        readConf = initializeReadconf(client)
        val unloadOp = client.startUnload(selectQuery, whereParams)
        val streamReader = new DataStreamReader(readConf, tableColumnMetadata)
        jobrefs.put(jobId, (unloadOp, client, streamReader))
        reader = streamReader
        closed = false
        logDebug("Started Vector unload")
      }
    })
    
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = if (jobrefs.contains(jobEnd.jobId)) {
      val (unloadOperation, client, streamReader) = jobrefs.remove(jobEnd.jobId).get
      try {
        Await.result(unloadOperation, Duration.create(100, "ms"))
      } catch {
        case e: Exception => {
          logDebug("Client disconnected while waiting for Vector unload to complete " + e.toString())
          for ( p <- 0 until readConf.size) {
            try {
              logDebug(s"Finalizing partition $p Vector transfer datastream")
              streamReader.touch(p) //Need to ensure all the unused streams have been closed
            } catch {
              case f: Exception => logDebug("Exception while finalizing Vector transfer datastream " + f.toString())
            }
          }
        }
      } finally {
        client.close
        logDebug(s"Unload vector job ended @ ${jobEnd.time}.")
      }
    }
  }
  sc.addSparkListener(vectorJobqueue)
  
  private var _readConf: VectorEndpointConf = VectorEndpointConf(IndexedSeq())
  protected def readConf = _readConf
  protected def readConf_= (value: VectorEndpointConf):Unit = _readConf = value
  
  private var _reader: DataStreamReader = _
  protected def reader = _reader
  protected def reader_= (value: DataStreamReader):Unit = _reader = value

  override protected def getPartitions: Array[Partition] = {
    if (vpartitions == 0) {
      // Need to create sample reader to determine default number of partitions if unspecified
      val client = new DataStreamClient(connectionProps, table)
      readConf = initializeReadconf(client)
      client.close()
      vpartitions = readConf.size
    }
    (0 until vpartitions).map(idx => new Partition { def index = idx }).toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    if (split.index == 0) {
      // Only need to setup once per read for all partitions
      vectorJobqueue.readyJob()
    }
    Seq(readConf.vectorEndpoints(split.index).host)
  }
  
  override def compute(split: Partition, taskContext: TaskContext): Iterator[InternalRow] = {
    taskContext.addTaskCompletionListener { _ => closeAll() }
    taskContext.addTaskFailureListener { (_, e) => closeAll(Option(e)) }
    it = reader.read(split.index)
    it
  }
  
  private def initializeReadconf(client: DataStreamClient): VectorEndpointConf = {
    client.prepareUnloadDataStreams(vpartitions)
    val conf =  client.getVectorEndpointConf
    logDebug(s"Configured ${conf.size} Vector endpoints for unloading")
    conf
  }

  private def closeAll(failure: Option[Throwable] = None): Unit = if (!closed) {
    failure.foreach(logError("Failure during task completion, closing RowReader", _))
    close(it, "RowReader")
    closed = true
  } else {
    failure.foreach(logError("Failure during task completion", _))
  }

  private def close[T <: { def close() }](c: T, resourceName: String): Unit = if (!closed && c != null) {
    try { c.close } catch { case e: Exception => logWarning(s"Exception closing $resourceName", e) }
  }
}
