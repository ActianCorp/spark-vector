package com.actian

/**
 * Spark-VectorH connector.
 *
 * With this connector, data can be loaded from Spark sources into `Vector(H)` and results of `Vector(H)` computations can be consumed in `Spark` and transformed into
 * a `DataFrame`. The first part is done in parallel: data coming from every input `RDD` partition is serialized using `Vector(H)'s` binary protocol and transfered
 * through socket connections to `Vector(H)` end points. Although there is a chance that network communication is incurred at this point, most of the time this connector
 * will try to assign only local `RDD` partitions to each `Vector(H)` end point. On the other side, data is currently exported from `Vector(H)` and ingested into `Spark`
 * using a JDBC connection to the leader `Vector(H)` node. The code that also permits this second part to be executed in parallel will soon be added.
 *
 * Throughout the documentation we will use `DataStream` and `Vector` end point interchangeably. A `Vector(H) DataStream` is the logical stream of consuming binary data in
 * `Vector(H)`. Typically, these `DataStream`s will be executed in parallel (i.e. there will be as many threads as `DataStreams` allocated), but there will be cases when
 * a `Vector(H)` thread will handle multiple `DataStreams`. On the other hand, each connection to a `Vector(H)` end point maps to exactly one `DataStream`.
 */
package object spark_vectorh {
}
