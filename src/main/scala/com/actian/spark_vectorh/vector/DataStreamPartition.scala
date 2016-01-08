package com.actian.spark_vectorh.vector

import java.io.{ IOException, ObjectOutputStream }

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
  /** An array of parent partitions to be mapped (through a `NarrowDependency` to this `DataStreamPartition` */
  var parents = parentIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    parents = parentIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }
}
