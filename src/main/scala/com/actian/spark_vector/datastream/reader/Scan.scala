/*
 * Copyright 2021 Actian Corporation
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
 *
 * Maintainer: francis.gropengieser@actian.com
 */

package com.actian.spark_vector.datastream.reader

import com.actian.spark_vector.datastream.DataStreamClient
import com.actian.spark_vector.datastream.DataStreamConnector
import com.actian.spark_vector.datastream.VectorEndpointConf
import com.actian.spark_vector.util.ResourceUtil.closeResourceAfterUse
import com.actian.spark_vector.util.ResourceUtil.closeResourceOnFailure
import com.actian.spark_vector.vector.ColumnMetadata
import com.actian.spark_vector.vector.VectorConnectionProperties

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.RDDInfo

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/* Reusable Vector Scan RDD which can also be used in the course of writing data back to Vector.
 * TODO (francis): Think about using wait/notify instead of busy waiting in the future.
*/
class Scan(
    @transient private val sc: SparkContext,
    val connectionProps: VectorConnectionProperties,
    val table: String,
    val tableColumnMetadata: Seq[ColumnMetadata],
    val selectQuery: String,
    val whereParams: Seq[Any] = Nil,
    private var vpartitions: Int = 0
) extends RDD[InternalRow](sc, Nil) {

  private val accumulator = sc.collectionAccumulator[Int]
  private var dataStreams: VectorEndpointConf = null
  /* Required in order to synchronize on a triggered computation. */
  private var computationTriggered: AtomicBoolean = new AtomicBoolean(false)
  @transient private var client: DataStreamClient = null

  sc.addSparkListener(new SparkListener {
    private val jobrefs =
      new HashMap[Int, (Future[Int], DataStreamClient, DataStreamReader)]
    private var jobId: Option[Int] = None
    /* Required in case of a cached RDD where the job is started, but no computation is triggered. */
    private var jobStartTask: Future[Unit] = null
    private var stopJobStartTask: AtomicBoolean = new AtomicBoolean(false)

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      if (jobStart.stageInfos.exists(_.rddInfos.exists(_.id == Scan.this.id))) {
        jobId = Some(jobStart.jobId)
        jobStartTask = Future {
          while (!computationTriggered.get() && !stopJobStartTask.get()) {
            Thread.sleep(1)
          }
          if (!stopJobStartTask.get()) {
            jobrefs.put(
              jobStart.jobId,
              (
                client.startUnload(selectQuery, whereParams),
                client,
                new DataStreamReader(dataStreams, tableColumnMetadata)
              )
            )
          }
        }
      }
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      if (jobId.isDefined && jobId.get == jobEnd.jobId) {
        logDebug(s"Job ${jobEnd.jobId} cleanup")

        jobId = None
        if (!computationTriggered.get()) {
          stopJobStartTask.set(true)
          return
        }
        val (unloadOperation, tmpclient, reader) =
          jobrefs.remove(jobEnd.jobId).get
        try {
          Await.result(unloadOperation, Duration.create(100, "ms"))
        } catch {
          case e: Exception => {
            logDebug(
              "Client could not cleanly disconnect from Vector, manually closing datastreams"
            )
            val untouched = List
              .range(0, dataStreams.size)
              .diff(accumulator.value.asScala.toList)
            untouched.foreach(p =>
              try {
                reader.touch(
                  p
                )
                logDebug(s"Closed partition $p Vector transfer datastream")
              } catch {
                case e: Exception =>
                  logDebug(
                    "Exception while closing unused Vector transfer datastream " + e
                      .toString()
                  )
              }
            )
          }
        } finally {
          tmpclient.close()
          logDebug(s"Unload vector job ${jobEnd.jobId} ended @ ${jobEnd.time}.")
          accumulator.reset()
          dataStreams = null
          computationTriggered.set(false)
        }
      }
    }
  })

  private def initDataStreams(): Unit = {
    client = new DataStreamClient(connectionProps, table)
    closeResourceOnFailure(client) {
      if (vpartitions == 0) {
        val defaultNumberOfPartitions = sc.defaultParallelism
        var maxNumberOfExecutors = defaultNumberOfPartitions
        if (sc.getConf.contains("spark.executor.instances")) {
          maxNumberOfExecutors =
            sc.getConf.get("spark.executor.instances").toInt
        }
        vpartitions =
          scala.math.min(defaultNumberOfPartitions, maxNumberOfExecutors)
      }
      client.prepareUnloadDataStreams(vpartitions)
      dataStreams = client.getVectorEndpointConf
      logDebug(s"Configured ${dataStreams.size} Vector endpoints for unloading")
    }
  }

  private def close(c: RowReader): Unit = {
    if (c != null) {
      try { c.close }
      catch {
        case e: Exception => logWarning(s"Exception closing RowReader", e)
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    while (computationTriggered.get()) {
      Thread.sleep(1)
    }
    if (dataStreams == null) {
      initDataStreams()
    }
    (0 until dataStreams.size)
      .map(idx => new Partition { def index = idx })
      .toArray
  }

  /* This method is always called before (re-)computation in contrast to getPartitions. */
  override protected def getPreferredLocations(
      split: Partition
  ): Seq[String] = {
    if (split.index == 0) {
      while (computationTriggered.get()) {
        Thread.sleep(1)
      }
      if (dataStreams == null) {
        initDataStreams()
      }
      computationTriggered.set(true)
    }
    val res = Seq(dataStreams.vectorEndpoints(split.index).host)
    res
  }

  override def compute(
      split: Partition,
      taskContext: TaskContext
  ): Iterator[InternalRow] = {
    var it: RowReader = null
    taskContext.addTaskCompletionListener[Unit]({ _ =>
      {
        if (it != null) {
          close(it)
          it = null;
        }
      }
    })

    taskContext.addTaskFailureListener({ (_, e) =>
      {
        logError("Failure during task completion, closing RowReader", e)
        if (it != null) {
          close(it)
          it = null;
        }
      }
    })

    try {
      val reader = new DataStreamReader(dataStreams, tableColumnMetadata)
      it = reader.read(split.index)
      accumulator.add(split.index)
      it
    } catch {
      case e: Exception =>
        logDebug(
          "Exception occurred when attempting to read from stream. If termination was abnormal an additional exception will be thrown.",
          e
        )
        Iterator.empty
    }
  }
}