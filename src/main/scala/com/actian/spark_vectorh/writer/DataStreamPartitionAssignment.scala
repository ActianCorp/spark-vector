package com.actian.spark_vectorh.writer

import scala.collection.mutable.{ ArrayBuffer, IndexedSeq => mutableISeq, Stack }

import org.apache.spark.Logging

/**
 * Class that contains the matching algorithm used to assign `RDD` partitions to Vector hosts, based on `affinities`.
 *
 * The algorithm used here tries to assign partitions to hosts for which they have affinity. For this reason only partitions
 * that have affinity to at least one host enter are matched here, the others are assigned to a random node. Also, this algorithm
 * aims to minimize the maximum number of partitions that a host will have assigned, i.e. the most data a host will process
 *
 * This algorithm is very similar to Hopcroft-Karp's matching algorithm in bipartite graphs.
 *  - Construct the bipartite graph where on the left side we have the partitions, on the right the hosts
 *  and the edges represent an affinity of a partition to a particular node
 *  - create an initial matching
 *  - define as `target` the ideal solution, i.e. `target` = `numPartitions / numHosts` rounded up
 *  - for each host, define as `g(host)` as the number of partitions currently assigned to `host`
 *  - separate the hosts into three classes: hosts with `g(host) > target, = target and < target`
 *  - At each iteration try to find an alternating path starting from a node with `g(host) > target` to a node that has `g(host) < target`.
 *  By negating this alternate path, each partition is still assigned to a node it has affinity to, but the sum of `|g(host) - target|`
 *  is smaller.
 *  - When there is no such alternating path anymore, we have reached the optimal solution
 *  - By trying to find more than one alternating path at each iteration, the complexity of the algorithm is improved to
 *  `|affinities|sqrt(numPartitions + numHosts)`. Since there is usually a constant number of hosts a partition has affinity to, e.g.
 *  replication factor, and numHosts is usually << numPartitions, the complexity of this algorithm is in fact `O(numPartitions sqrt(numPartitions))`
 *
 *  @param numPartitions Number of partitions of input RDD
 *  @param numHosts Number of hosts (Vector hosts)
 *  @param affinities Affinities of each partition to hosts (represented by their integer index from `[0, numHosts)`)
 */
class DataStreamPartitionAssignment(numPartitions: Int, numHosts: Int, affinities: IndexedSeq[IndexedSeq[Int]]) extends Logging {
  private[this] val matchingHost = mutableISeq.fill(numPartitions)(-1)
  private[this] val numPartitionsPerHost = mutableISeq.fill(numHosts)(0)
  private[this] val hostToPartitions = {
    for (i <- 0 until numPartitions; j <- 0 until affinities(i).size)
      numPartitionsPerHost(affinities(i)(j)) += 1
    val ret = mutableISeq.tabulate(numHosts)(host => mutableISeq.fill(numPartitionsPerHost(host))(0))
    for (i <- 0 until numHosts) numPartitionsPerHost(i) = 0
    for (i <- 0 until numPartitions; j <- 0 until affinities(i).size) {
      val host = affinities(i)(j)
      ret(host)(numPartitionsPerHost(host)) = i
      numPartitionsPerHost(host) += 1
    }
    for (i <- 0 until numHosts) numPartitionsPerHost(i) = 0
    ret
  }

  private[this] val st = Stack.empty[(Int, Int)]

  private val target = numPartitions / numHosts + (if (numPartitions % numHosts != 0) 1 else 0)

  private def findPath(part: Int, offset: IndexedSeq[Iterator[Int]], visited: mutableISeq[Boolean]): Boolean = {
    st.push((part, -1))
    var foundPath = false
    while (!st.isEmpty) {
      val (part, afidx) = st.pop
      if (foundPath) matchingHost(part) = affinities(part)(afidx)
      if (afidx == -1) {
        visited(part) = true
        affinities(part).find {
          case host =>
            if (host != matchingHost(part) && numPartitionsPerHost(host) < target) {
              foundPath = true
              numPartitionsPerHost(host) += 1
              matchingHost(part) = host
            }
            foundPath
        }
      }
      if (!foundPath && afidx + 1 < affinities(part).size) {
        val host = affinities(part)(afidx + 1)
        var nextPart = -1
        while (offset(host).hasNext && (nextPart == -1 || visited(nextPart) || matchingHost(nextPart) != host)) {
          nextPart = offset(host).next
        }
        if (nextPart != -1 && !visited(nextPart) && matchingHost(nextPart) == host) {
          st.push((part, afidx + 1))
          st.push((nextPart, -1))
        }
      }
    }
    foundPath
  }

  /**
   * Get an assignment of partitions to hosts satisfying the properties described in the header
   *
   *  @return One sequence of partition indexes assigned to each host
   */
  def get: IndexedSeq[IndexedSeq[Int]] = {
    implicit val accs = profileInit("first assignment", "flow", "ret")
    profile("first assignment")
    /* Start with some assignment, any assignment */
    (0 to numPartitions - 1).foreach {
      case part =>
        val host = affinities(part).minBy(numPartitionsPerHost(_))
        matchingHost(part) = host
        numPartitionsPerHost(host) += 1
    }
    profileEnd
    val more = (0 to numHosts - 1).filter(numPartitionsPerHost(_) > target)

    val visited = mutableISeq.fill(numPartitions)(false)
    var repeat = true
    var iterations = 0
    val iterators = mutableISeq.fill(numHosts)(hostToPartitions(0).iterator)
    profile("flow")
    while (repeat) {
      iterations += 1
      (0 to numPartitions - 1).foreach { visited(_) = false }
      (0 to numHosts - 1).foreach { case host => iterators(host) = hostToPartitions(host).iterator }

      repeat = false
      more.foreach {
        case host =>
          while (iterators(host).hasNext && numPartitionsPerHost(host) > target) {
            val part = iterators(host).next
            if (matchingHost(part) == host &&
              !visited(part) && findPath(part, iterators, visited)) {
              numPartitionsPerHost(host) -= 1
              repeat = true
            }
          }
      }
    }
    profileEnd
    log.debug(s"Matching algorithm took $iterations iterations")
    profile("ret")
    val hostsWithPartitions = (0 until numPartitions).view.groupBy(matchingHost(_)).mapValues(_.force)
    val ret = (0 until numHosts).map(host => hostsWithPartitions.get(host).getOrElse(IndexedSeq.empty[Int]))
    profileEnd
    profilePrint(log)
    ret
  }
}

object DataStreamPartitionAssignment extends Logging {
  def getAssignmentToVectorEndpoints(affinities: Array[_ <: Seq[String]], endpoints: IndexedSeq[VectorEndPoint]): IndexedSeq[IndexedSeq[Int]] = {
    implicit val accs = profileInit("preparing structures", "assignment to hosts", "translating to assignment to endpoints", "assigning partitions without affinity")
    profile("preparing structures")
    val (partitionsWithAffinity, partitionsWithoutAffinity) = (0 to affinities.size - 1).map { part => (affinities(part), part) }.partition(!_._1.isEmpty)
    val numPartitions = partitionsWithAffinity.size
    val hosts = endpoints.map(_.host).distinct.zipWithIndex.toMap
    val endPointsPerHost = (0 to endpoints.size - 1).view.groupBy(e => hosts(endpoints(e).host))
    val numEndpointsPerHost = endPointsPerHost.mapValues(_.size)
    val affinitiesToHosts = (0 to numPartitions - 1).map { idx =>
      partitionsWithAffinity(idx)._1.map { case hname => hosts(hname) }.toIndexedSeq
    }
    profileEnd

    log.debug(s"data stream partition assignment: trying to assign ${affinities.size} partitions to ${endpoints.size} endpoints")
    log.debug(s"data stream partition assignment: ${partitionsWithAffinity.size} partitions have affinity, ${partitionsWithoutAffinity.size} don't")

    profile("assignment to hosts")
    var partitionsPerHost = (new DataStreamPartitionAssignment(numPartitions, hosts.size, affinitiesToHosts)).get

    var sanity = 0
    (0 to hosts.size - 1).view.map {
      case host =>
        partitionsPerHost(host).view.map(partitionsWithAffinity(_)._2).foreach {
          case partition =>
            if (affinities(partition).find(hosts(_) == host).isEmpty) sanity += 1
        }
    }
    log.debug(s"After matching algorithm, $sanity partitions are read remotely")
    profileEnd

    profile("translating to assignment to endpoints")
    val partitionsAssignedPerHost = mutableISeq.tabulate(hosts.size)(partitionsPerHost(_).size)
    val ret = mutableISeq((0 to hosts.size - 1).view.flatMap {
      case host =>
        val partitions = partitionsPerHost(host)
        var i = 0
        val numEndpoints = numEndpointsPerHost(host)
        (0 to numEndpoints - 1).view.map {
          case idx =>
            val endpoint = endPointsPerHost(host)(idx)
            val share = (partitions.size / numEndpoints) + (if (idx < partitions.size % numEndpoints) 1 else 0)
            val ret = partitions.slice(i, i + share).map(partitionsWithAffinity(_)._2)
            i += share
            ArrayBuffer(ret: _*)
        }.force
    }.force: _*)
    profileEnd

    profile("assigning partitions without affinity")
    val maxPartPerEndpoint = ret.map(_.size).reduce(_ max _)
    var i = 0
    ret.foreach {
      case partitions =>
        if (partitions.size < maxPartPerEndpoint) {
          val toAdd = Math.min(partitionsWithoutAffinity.size - i, maxPartPerEndpoint - partitions.size)
          partitions ++= partitionsWithoutAffinity.slice(i, i + toAdd).map(_._2)
          i += toAdd
        }
    }
    var k = 0
    (i to partitionsWithoutAffinity.size - 1).foreach {
      case idx =>
        ret(k) += partitionsWithoutAffinity(idx)._2
        k = if (k + 1 >= ret.size) 0 else k + 1
    }
    sanity = 0
    (0 to endpoints.size - 1).view.foreach { endpoint => ret(endpoint).foreach { case partition => if (affinities(partition).find(endpoints(endpoint).host == _).isEmpty) sanity += 1 } }
    log.debug(s"After computing assignment, $sanity partitions are read remotely")
    profileEnd
    profilePrint(log)
    ret
  }
}
