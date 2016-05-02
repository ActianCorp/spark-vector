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
package com.actian

/**
 * Spark-Vector connector.
 *
 * With this connector, data can be loaded from Spark sources into `Vector` and results of `Vector` computations can be consumed in `Spark` and transformed into
 * a `DataFrame`. The first part is done in parallel: data coming from every input `RDD` partition is serialized using `Vector's` binary protocol and transfered
 * through socket connections to `Vector` end points. Although there is a chance that network communication is incurred at this point, most of the time this connector
 * will try to assign only local `RDD` partitions to each `Vector` end point. On the other side, data is currently exported from `Vector` and ingested into `Spark`
 * using a JDBC connection to the leader `Vector` node. The code that also permits this second part to be executed in parallel will soon be added.
 *
 * Throughout the documentation we will use `DataStream` and `Vector` end point interchangeably. A `Vector DataStream` is the logical stream of consuming binary data in
 * `Vector`. Typically, these `DataStream`s will be executed in parallel (i.e. there will be as many threads as `DataStreams` allocated), but there will be cases when
 * a `Vector` thread will handle multiple `DataStreams`. On the other hand, each connection to a `Vector` end point maps to exactly one `DataStream`.
 */
package object spark_vector {
  /**
   * Having two `PartialFunctions` `f` and `g` with no side effects, we compose them into another partial function `h` such that
   * `h.isDefinedAt(x)` if `f.isDefinedAt(x)` and `g.isDefinedAt(f(x))`
   * @return a `PartialFunction` composed from `f` and `g`
   */
  implicit class ComposePartial[A, B](f: PartialFunction[A, B]) {
    def andThenPartial[C](g: PartialFunction[B, C]): PartialFunction[A, C] = Function.unlift(f.lift(_) flatMap g.lift)
  }

  implicit class BooleanExpr(expr: Boolean) {
    def ifThenElse[T](thenVal: => T, elseVal: => T): T = if (expr) thenVal else elseVal
  }
}
