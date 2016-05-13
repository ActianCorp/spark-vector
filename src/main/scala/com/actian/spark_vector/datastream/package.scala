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
 * Loading from `Spark` to `Vector` will be initiated through the `SparkSQL`/`DataFrames` APIs.
 * The sequence of operations is:
 *  - Given an input `RDD` with its corresponding data type information, its fields will either be matched to existing table's columns or they will help generate a
 *  `create table` SQL statement that will be first submitted to `Vector`.
 *  - Helpers [[RowWriter]] and [[DataStreamWriter]] objects are created and they contain all the needed information for a `Spark` worker to be able to process,
 *  serialize and write binary data to `Vector` end points.
 *  - An [[InsertRDD]] is created, containing as many partitions as there are `DataStreams` and that will create a `NarrowDependency` to the input `RDD`
 *  - Driver initiates the load, issuing a SQL query to `Vector` leader node
 *  - Driver initiates Spark job => [[RowWriter]] and [[DataStreamWriter]] objects, part of the closure, are serialized and sent to worker processes
 *  - Each worker process reads its corresponding write configuration and starts processing input data (as assigned by the driver when [[InsertRDD]] was created), serializes it into
 *  `ByteBuffers` and then flushes them through the socket towards one (and only one) predetermined `Vector` end point
 *  - During this time, the driver remains blocked waiting for the SQL query to finish. Once all workers are done, the driver then issues a `commit` or `abort` depending on whether any of the
 *  workers failed. Note, we currently do not retry `Spark` workers since partial loading is not supported in `Vector` yet.
 *
 * Unloading from `Vector` to `Spark` will be initiated through the `SparkSQL`/`DataFrames` APIs.
 * The sequence of operations is:
 *  - Given a `SparkSQL` select query, a [[ScanRDD]] is created containing as many partitions as there are `DataStreams`
 *  - Helpers [[RowReader]] and [[DataStreamReader]] objects are created and they contain all the needed information for a `Spark` worker to be able to read binary data from `Vector`
 *  end points, deserialize and process it
 *  - Driver initiates the unload, issuing a SQL query to `Vector` leader node
 *  - Driver initiates Spark job => [[RowReader]] and [[DataStreamReader]] objects, part of the closure, are serialized and sent to worker processes
 *  - Each worker process reads its corresponding read configuration and starts processing output data (as assigned by the driver when [[ScanRDD]] was created), deserializes it into
 *  `ByteBuffers` and then through an `Iterator[Row]` we can collect the data row by row
 *  - During this time, the driver remains blocked waiting for the SQL query to finish. Once all workers are done, the driver then issues a `commit` or `abort` depending on whether any of the
 *  workers failed. Note, we currently do not retry `Spark` workers since partial loading is not supported in `Vector` yet.
 */
package object datastream {
  /** Helper to determine how much padding (# of trash bytes) needs to be written to properly align a type with size `typeSize`, given that we are currently at `pos` */
  def padding(pos: Int, typeSize: Int): Int = if ((pos & (typeSize - 1)) != 0) {
    typeSize - (pos & (typeSize - 1))
  } else {
    0
  }
}
