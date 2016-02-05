package com.actian.spark_vectorh.writer

import scala.annotation.tailrec
import scala.reflect.ClassTag

import org.apache.spark.{ Logging, OneToOneDependency, NarrowDependency, Partition, TaskContext }
import org.apache.spark.rdd.RDD

import com.actian.spark_vectorh.vector.DataStreamPartition

/**
 * `Vector(H)` RDD to load data into `Vector(H)` through its `DataStream API`
 *
 *  @param rdd `RDD` to be loaded
 *  @param writeConf contains the write configuration needed to connect to `Vector(H) DataStream`s
 */
class DataStreamRDD[R: ClassTag](
  @transient val rdd: RDD[R],
  writeConf: WriteConf) extends RDD[R](rdd.context, Nil) with Logging {

  /** All hosts where `VectorH` expects data to be loaded */
  private val vectorHosts = writeConf.vectorEndPoints.map(_.host).toSet
  /** Used for logging what partitions are assigned to which `DataStream` */
  private val partitionsPerDataStreamToPrint = 10

  /** Obtain the preferred locations of a partition, eventually looking into grand children `RDD`s as long as the dependencies traversed are OneToOne */
  @tailrec
  private def getPreferredLocationsRec(rdd: RDD[R], partition: Partition): Seq[String] = {
    val locations = rdd.preferredLocations(partition)
      .filter(vectorHosts.contains(_))

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

    val ret = DataStreamPartitionAssignment.getAssignmentToVectorEndpoints(affinities, writeConf.vectorEndPoints)

    logDebug(s"Computed endPointsToParentPartitionsMap and got ..." +
      s"""${
        (0 until ret.length).map {
          case idx =>
            val vals = ret(idx)
            s"Datastream $idx -> RDD partitions ${vals.length}: ${vals.take(partitionsPerDataStreamToPrint).mkString(",")} ${if (vals.length > partitionsPerDataStreamToPrint) "..." else ""}"
        }
      }""")
    ret.map(_.map(rdd.partitions(_).index))
  }

  override protected def getPartitions = (0 to writeConf.vectorEndPoints.length - 1)
    .map(x => DataStreamPartition(x, rdd, endPointsToParentPartitionsMap(x)))
    .toArray

  override protected def getPreferredLocations(split: Partition) = {
    logDebug(s"getPreferredLocations is called for partition ${split.index} and we are returning ${writeConf.vectorEndPoints(split.index).host}")
    Seq(writeConf.vectorEndPoints(split.index).host)
  }

  override def compute(split: Partition, task: TaskContext): Iterator[R] = {
    //logTrace(s"Computing partition ${split.index} = ${task.partitionId}")
    split.asInstanceOf[DataStreamPartition].parents.toIterator.flatMap(firstParent[R].iterator(_, task))
  }

  override def getDependencies: Seq[NarrowDependency[_]] =
    Seq(new NarrowDependency(rdd) {
      def getParents(partitionId: Int) = endPointsToParentPartitionsMap(partitionId)
    })
}
