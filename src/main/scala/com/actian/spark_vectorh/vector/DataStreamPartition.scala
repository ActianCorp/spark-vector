package com.actian.spark_vectorh.vector

import java.io.{ IOException, ObjectOutputStream }

import scala.language.existentials

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD

case class DataStreamPartition(index: Int, @transient rdd: RDD[_], parentIndices: Seq[Int]) extends Partition {
  var parents = parentIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    parents = parentIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }
}
