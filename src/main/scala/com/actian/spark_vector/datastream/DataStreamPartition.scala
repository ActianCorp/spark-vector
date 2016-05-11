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
package com.actian.spark_vector.datastream

import java.io.ObjectOutputStream

import scala.language.existentials

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD

/**
 * A partition corresponding to one `DataStream`
 *
 * @param parentIndices a list of indices of the parent partitions that will be assigned to this `DataStream` and will
 * subsequently be serialized and sent through a single connection
 */
case class DataStreamPartition(index: Int, @transient rdd: RDD[_], parentIndices: Seq[Int]) extends Partition {
  /** An array of parent partitions to be mapped (through a `NarrowDependency` to this `DataStreamPartition`) */
  var parents = parentIndices.map(rdd.partitions(_))

  private def writeObject(oos: ObjectOutputStream) {
    /** Update the reference to parent split at the time of task serialization */
    parents = parentIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }
}
