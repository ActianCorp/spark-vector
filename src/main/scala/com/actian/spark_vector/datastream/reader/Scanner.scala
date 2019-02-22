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
import org.apache.spark.sql.catalyst.InternalRow

import com.actian.spark_vector.datastream.VectorEndpointConf

/**
 * `Vector` RDD to load data into `Spark` through `Vector`'s `Datastream API` when the endpoint connections are known ahead of time
 */
class Scanner(@transient private val sc: SparkContext, 
              val readConf: VectorEndpointConf, 
              val reader: DataStreamReader) 
              extends RDD[InternalRow](sc, Nil) {
  
  /** Custom row iterator for reading `DataStream`s in row format */
  @volatile private var it: RowReader = _
  
  override protected def getPartitions = (0 until readConf.size).map(idx => new Partition { def index = idx }).toArray

  override protected def getPreferredLocations(split: Partition) = Seq(readConf.vectorEndpoints(split.index).host)
  
  override def compute(split: Partition, taskContext: TaskContext): Iterator[InternalRow] = {
    taskContext.addTaskCompletionListener { _ => closeAll() }
    taskContext.addTaskFailureListener { (_, e) => closeAll(Option(e)) }
    logDebug("Computing partition " + split.index)
    try {
      it = reader.read(split.index)
      it
    } catch { case e: Exception => 
      logDebug("Exception occurred when attempting to read from stream. If termination was abnormal an additional exception will be thrown.", e)
      Iterator.empty
    }
  }
  
  def touchDatastreams(parts: List[Int] = List[Int]()) {
    val untouched = List.range(0, readConf.size).diff(parts)
    untouched.foreach ( p =>
      try {
        reader.touch(p) //Need to ensure all the streams have been closed except the one used by this instance
        logDebug(s"Closed partition $p Vector transfer datastream")
      } catch {
        case e: Exception => logDebug("Exception while closing unused Vector transfer datastream " + e.toString())
      }
    )
  }
  
  def closeAll(failure: Option[Throwable] = None): Unit = {
    failure.foreach(logError("Failure during task completion, closing RowReader", _))
    if (it != null) {
      close(it, "RowReader")
      it = null;
    }
  }
  
  private def close[T <: { def close() }](c: T, resourceName: String): Unit = if (c != null) {
    try { c.close } catch { case e: Exception => logWarning(s"Exception closing $resourceName", e) }
  }
  
}