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
package com.actian.spark_vector.datastream.writer

import scala.annotation.tailrec
import scala.reflect.ClassTag

import org.apache.spark.{ Logging, OneToOneDependency, NarrowDependency, Partition, TaskContext }
import org.apache.spark.rdd.RDD

import com.actian.spark_vector.datastream.{ DataStreamPartition, DataStreamPartitionAssignment, VectorEndpointConf }

/**
 * `Vector` RDD to load data into `Vector` through its `DataStream API`
 *
 * @param rdd `RDD` to be loaded
 * @param writeConf contains the write configuration needed to connect to `Vector DataStream`s
 */
class InsertRDD[R: ClassTag](@transient val rdd: RDD[R], writeConf: VectorEndpointConf) extends RDD[R](rdd.context, Nil) with Logging {
  /** All hosts where `Vector` expects data to be loaded */
  private val vectorHosts = writeConf.vectorEndpoints.map(_.host).toSet
  /** Used for logging what partitions are assigned to which `DataStream` */
  private val partitionsPerDataStreamToPrint = 10

  /** Obtain the preferred locations of a partition, eventually looking into grand children `RDD`s as long as the dependencies traversed are OneToOne */
  @tailrec
  private def getPreferredLocationsRec(rdd: RDD[R], partition: Partition): Seq[String] = {
    val locations = rdd.preferredLocations(partition).filter(vectorHosts.contains(_))
    if (!locations.isEmpty) {
      locations
    } else {
      rdd.dependencies match {
        case Seq(x: OneToOneDependency[R]) => {
          val parentRDD = x.rdd
          val parentIndex = x.getParents(partition.index).head
          getPreferredLocationsRec(parentRDD, parentRDD.partitions(parentIndex))
        }
        case _ => locations
      }
    }
  }

  /** Optimally assign RDD partitions to DataStreams, taking into account partition affinities */
  private val endPointsToParentPartitionsMap = {
    val affinities = rdd.partitions.map {
      case partition =>
        getPreferredLocationsRec(rdd, partition)
    }

    val ret = DataStreamPartitionAssignment(affinities, writeConf.vectorEndpoints)
    logDebug(s"Computed endPointsToParentPartitionsMap and got: ${(0 until ret.length).map {
      case idx =>
        val vals = ret(idx)
        s"Datastream $idx -> RDD partitions ${vals.length}: ${vals.take(partitionsPerDataStreamToPrint).mkString(",")} ${if (vals.length > partitionsPerDataStreamToPrint) "..." else ""}"
    }}")
    ret.map(_.map(rdd.partitions(_).index))
  }

  override protected def getPartitions = (0 until writeConf.vectorEndpoints.length).map(x =>
    DataStreamPartition(x, rdd, endPointsToParentPartitionsMap(x))).toArray

  override protected def getPreferredLocations(split: Partition) = {
    logDebug(s"getPreferredLocations is called for partition ${split.index} and we are returning ${writeConf.vectorEndpoints(split.index).host}")
    Seq(writeConf.vectorEndpoints(split.index).host)
  }

  override def compute(split: Partition, taskContext: TaskContext): Iterator[R] =
    split.asInstanceOf[DataStreamPartition].parents.toIterator.flatMap(firstParent[R].iterator(_, taskContext))

  override def getDependencies: Seq[NarrowDependency[_]] = Seq(new NarrowDependency(rdd) {
    def getParents(partitionId: Int) = endPointsToParentPartitionsMap(partitionId)
  })
}
