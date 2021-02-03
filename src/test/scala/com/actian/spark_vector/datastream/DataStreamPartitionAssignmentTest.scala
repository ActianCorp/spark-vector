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

import scala.util.Random

import org.scalacheck.Gen
import org.scalacheck.Prop.{ forAllNoShrink, propBoolean }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._
import org.scalatestplus.scalacheck.Checkers._

import com.actian.spark_vector.util.Logging
import com.actian.spark_vector.test.tags.RandomizedTest

class TestBipartiteAssignment(
    override protected val nA: Int,
    override protected val nB: Int,
	override protected val edges: IndexedSeq[IndexedSeq[Int]]
) extends BipartiteAssignment {
  override protected val target: Seq[Int] = Seq.fill(nB)(nA / nB)
}

/** Tests of DataStreamPartitionAssignment */
class DataStreamPartitionAssignmentTest extends AnyFunSuite with Matchers with Logging {
  private val perfectMatchParamsGen = for {
    nB <- Gen.choose(1, 60)
    nA <- Gen.choose(1, 24).map(_ * nB) /* to be sure they split evenly */
    replFactor <- Gen.choose(1, Math.ceil(nB.toDouble / 2).toInt)
    edges <- Gen.listOfN(nA, Gen.listOfN(replFactor - 1, Gen.choose(0, nB - 1)))
      .map(l => (0 until l.size).map(a => Random.shuffle((l(a) :+ (a % nB)).distinct.toIndexedSeq))) /* make sure that there is a match that evenly splits the partitions among nodes */
  } yield (nA, nB, edges)

  private def generateEndPoints(nA: Int) = for {
    i <- 0 until nA
    endpoint = VectorEndpoint(s"host-$i", 1, "someuser", "somepassword")
  } yield endpoint

  private val lessPartitionsThanDataStreamsGen = for {
    nB <- Gen.choose(3, 30)
    nEndpoints <- Gen.choose(nB, 400)
    nA <- Gen.choose(1, nEndpoints - 1)
    replFactor <- Gen.choose(1, 4)
    /** no affinity */
    edges <- Gen.listOfN(nA, Seq.empty[String])
  } yield (edges, generateEndPoints(nEndpoints))

  private val randomMatchParamsGen = for {
    nB <- Gen.choose(3, 50)
    nA <- Gen.choose(0, 1200)
    replFactor <- Gen.choose(1, 10)
    /** make partitions have affinity to nodes that are not necessarily VectorEndpoints */
    edges <- Gen.listOfN(nA, Gen.resize(replFactor, Gen.listOf(Gen.choose(0, nB * 2 - 1).map(b => s"host-$b"))))
  } yield (edges, generateEndPoints(nB))

  private val unevenEndpointsPerHostGen = for {
    nB <- Gen.choose(3, 20)
    nA <- Gen.choose(0, 1200)
    /** all partitions are replicated on all nodes */
    edges = (0 until nA).map(a => (0 until nB).map(b => s"host-$b"))
    numEndpoints <- Gen.choose(3, 50)
    endpoints <- Gen.listOfN(numEndpoints, Gen.choose(0, nB - 1).map(b => VectorEndpoint(s"host-$b", 1, "someuser", "somepassword")))
  } yield (edges, endpoints.toIndexedSeq)

  test("finds the optimal (evenly split) match", RandomizedTest) {
    check(
      forAllNoShrink(perfectMatchParamsGen) {
        case (numA, numB, e) =>
          numA > 0 && numB > 0 ==> {
            val assignment = new TestBipartiteAssignment(numA, numB, e)
            val ret = assignment.matching.map(_.size)
            ret.max == numA / numB
          }
      },
      minSuccessful(1000))
  }

  test("finds the optimal match with less partitions than nodes", RandomizedTest) {
    check(
      forAllNoShrink(lessPartitionsThanDataStreamsGen) {
        case (edges, endpoints) =>
          val assignment = DataStreamPartitionAssignment(edges.toArray, endpoints)
          assignment.map(_.size).max == 1
      },
      minSuccessful(1000))
  }

  test("any random matching works without errors", RandomizedTest) {
    check(
      forAllNoShrink(randomMatchParamsGen) {
        case (edges, endpoints) =>
          val assignment = DataStreamPartitionAssignment(edges.toArray, endpoints)
          val ret = assignment.map(_.size).max - assignment.map(_.size).min <= DataStreamPartitionAssignment.MaxSkew
          ret
      },
      minSuccessful(1000))
  }

  test("matching detects unbalanced hosts", RandomizedTest) {
    check(
      forAllNoShrink(unevenEndpointsPerHostGen) {
        case (edges, endpoints) =>
          val assignment = DataStreamPartitionAssignment(edges.toArray, endpoints)
          assignment.map(_.size).max <= assignment.map(_.size).min + 1
      },
      minSuccessful(1000))
  }
}
