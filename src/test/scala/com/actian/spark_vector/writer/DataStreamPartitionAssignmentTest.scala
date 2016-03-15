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
package com.actian.spark_vector.writer

import scala.util.Random

import org.scalacheck.Gen
import org.scalacheck.Prop.{ forAllNoShrink, propBoolean }
import org.scalatest.{ Finders, FunSuite, Matchers }
import org.scalatest.prop.Checkers.{ check, generatorDrivenConfig, minSuccessful }

import com.actian.spark_vector.test.tags.RandomizedTest

/** Tests of DataStreamPartitionAssignment
 */
class DataStreamPartitionAssignmentTest extends FunSuite with Matchers {
  private val perfectMatchParamsGen = for {
    nB <- Gen.choose(1, 60)
    nA <- Gen.choose(1, 24).map(_ * nB) /* to be sure they split evenly */
    replFactor <- Gen.choose(1, Math.ceil(nB / 2).toInt)
    edges <- Gen.listOfN(nA, Gen.listOfN(replFactor - 1, Gen.choose(0, nB - 1)))
      .map(l => (0 until l.size).map(a => Random.shuffle((l(a) :+ (a % nB)).distinct.toIndexedSeq))) /* make sure that there is a match that evenly splits the partitions among nodes */
  } yield (nA, nB, edges)

  test("finds the optimal (evenly split) match", RandomizedTest) {
    check(
      forAllNoShrink(perfectMatchParamsGen) {
        case (numA, numB, e) =>
          numA > 0 && numB > 0 ==> {
            val assignment = new BipartiteAssignment {
              val nA = numA
              val nB = numB
              val edges = e
            }
            val ret = assignment.matching.map(_.size)
            ret.max == numA / numB
          }
      },
      minSuccessful(1000))
  }
}
