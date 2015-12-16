package com.actian.spark_vectorh.writer

import scala.annotation.tailrec
import scala.reflect.ClassTag

import org.apache.spark.{ Logging, OneToOneDependency, NarrowDependency, Partition, TaskContext }
import org.apache.spark.rdd.RDD

import com.actian.spark_vectorh.vector.DataStreamPartition

class DataStreamRDD[R: ClassTag](
    @transient val rdd: RDD[R],
    writeConf: WriteConf) extends RDD[R](rdd.context, Nil) with Logging {

  private val vectorHosts = writeConf.vectorEndPoints.map(_.host).toSet
  private val partitionsPerDataStreamToPrint = 10

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

  private val endPointsToParentPartitionsMap = {
    val affinities = rdd.partitions.map {
      case partition =>
        getPreferredLocationsRec(rdd, partition)
    }

    val ret = DataStreamPartitionAssignment.getAssignmentToVectorEndpoints(affinities, writeConf.vectorEndPoints)

    log.debug(s"Computed endPointsToParentPartitionsMap and got ..." +
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
    log.debug(s"getPreferredLocations is called for partition ${split.index} and we are returning ${writeConf.vectorEndPoints(split.index).host}")
    Seq(writeConf.vectorEndPoints(split.index).host)
  }

  override def compute(split: Partition, task: TaskContext): Iterator[R] = {
    //log.trace(s"Computing partition ${split.index} = ${task.partitionId}")
    split.asInstanceOf[DataStreamPartition].parents.toIterator.flatMap(firstParent[R].iterator(_, task))
  }

  override def getDependencies: Seq[NarrowDependency[_]] =
    Seq(new NarrowDependency(rdd) {
      def getParents(partitionId: Int) = endPointsToParentPartitionsMap(partitionId)
    })
}
