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

import scala.language.reflectiveCalls

import org.apache.spark.{ Logging, Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import com.actian.spark_vector.datastream.VectorEndpointConf

/**
 * `Vector` RDD to load data into `Spark` through JDBC
 *
 *  @param vectorSelectStatement Vector prepared SQL statement to be issued through JDBC to start exporting data
 *  @param vectorSelectParams  Parameters to the prepared SQL statement, if any
 */
class ScanRDD(@transient private val sc: SparkContext, reader: DataStreamReader) extends RDD[Row](sc, Nil) with Logging {
  private var closed = false
  private var it: RowReader = null

  override protected def getPartitions = (0 until reader.readConf.vectorEndpoints.size).map(i => new Partition { def index = i }).toArray

  override protected def getPreferredLocations(split: Partition) = Seq(reader.readConf.vectorEndpoints(split.index).host)

  override def compute(split: Partition, taskContext: TaskContext): Iterator[Row] = {
    taskContext.addTaskCompletionListener { _ =>
      close(it, "data stream reader")
      closed = true
    }
    it = reader.read(taskContext)
    it
  }

  private def close[T <: { def close() }](c: T, resourceName: String): Unit = if (!closed && c != null) {
    try { c.close } catch { case e: Exception => logWarning(s"Exception closing $resourceName", e) }
  }
}
