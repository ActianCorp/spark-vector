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

import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow

import com.actian.spark_vector.datastream.VectorEndpointConf

/**
 * `Vector` RDD to load data into `Spark` through `Vector`'s `Datastream API`
 */
class ScanRDD(@transient private val sc: SparkContext, readConf: VectorEndpointConf, reader: DataStreamReader) extends RDD[InternalRow](sc, Nil) {
  /** Closed state for the datastream connection */
  @volatile private var closed = false
  /** Custom row iterator for reading `DataStream`s in row format */
  @volatile private var it: RowReader = _

  override protected def getPartitions = (0 until readConf.vectorEndpoints.size).map(idx => new Partition { def index = idx }).toArray

  override protected def getPreferredLocations(split: Partition) = Seq(readConf.vectorEndpoints(split.index).host)

  override def compute(split: Partition, taskContext: TaskContext): Iterator[InternalRow] = {
    taskContext.addTaskCompletionListener { _ => closeAll() }
    taskContext.addTaskFailureListener { (_, e) => closeAll(Option(e)) }
    
    closed = false
    it = reader.read(split.index)
    it
  }
  
  def touchDatastreams() {
    for ( p <- 0 to readConf.vectorEndpoints.size - 1) {
      try {
        logDebug(s"Touching partition $p datastream")
        reader.touch(p) //Need to ensure all the streams have been 
      } catch {
        case e: Exception => logDebug(e.toString())
      }
    }
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
