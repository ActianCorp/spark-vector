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
package com.actian.spark_vector

/**
 * Spark -> Vector loading part
 *
 *  Loading from `Spark` to `Vector` will be initiated with a call to [[vector.LoadVector.loadVector]], either directly or through the `SparkSQL`/`DataFrames` APIs. The sequence of operations
 *  is:
 *   - given an input `RDD` with its corresponding data type information, its fields will either be matched to existing table's columns or they will help generate a
 *  `create table` SQL statement that will be first submitted to `Vector(H)`.
 *   - helpers [[RowWriter]] and [[DataStreamWriter]] objects are created and they contain all the needed information for a `Spark` worker to be able to process,
 *  serialize and communicate binary data to `Vector(H)` end points.
 *   - a [[DataStreamRDD]] is created, containing as many partitions as there are `DataStreams` and that will create a `NarrowDependency` to the input `RDD`
 *   - driver initiates the load, issuing a SQL query to `Vector(H)` leader node
 *   - driver initiates Spark job => [[RowWriter]] and [[DataStreamWriter]] objects, part of the closure, are serialized and sent to worker processes
 *   - each worker process reads its corresponding write configuration and starts processing input data (as assigned by the driver when [[DataStreamRDD]] was created), serializes it into
 *  `ByteBuffers` and then flushes them through the socket towards one (and only one) predetermined `Vector` end point
 *   - during this time, the driver remains blocked waiting for the SQL query to finish. Once all workers are done, the driver then issues a `commit` or `abort` depending on whether any of the
 *  workers failed. Note, we currently do not retry `Spark` workers since partial loading is not supported in `Vector(H)` yet.
 */
package object writer {
  val IntSize = 4
}
