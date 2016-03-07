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
package com.actian.spark_vector.reader

import scala.language.reflectiveCalls

import org.apache.spark.{ Logging, Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry

import com.actian.spark_vector.vector.{ ResultSetRowIterator, VectorConnectionProperties, VectorJDBC }

/**
 * `Vector` RDD to load data into `Spark` through JDBC
 *
 *  @param connProps contains the read configuration needed to connect to the Vector front-end
 *  @param vectorSelectStatement Vector prepared SQL statement to be issued through JDBC to start exporting data
 *  @param vectorSelectParams  Parameters to the prepared SQL statement, if any
 */
class ScanRDD(
    @transient private val sc: SparkContext,
    connProps: VectorConnectionProperties,
    vectorSelectStatement: String,
    vectorSelectParams: Seq[Any] = Nil) extends RDD[Row](sc, Nil) with Logging {
  private var closed = false
  private var it: ResultSetRowIterator = null

  override protected def getPartitions = Array(new Partition { def index = 0 })

  override protected def getPreferredLocations(split: Partition) = Seq(connProps.host)

  override def compute(split: Partition, task: TaskContext): Iterator[Row] = {
    /* Need to register ingres JDBC driver so that it is picked up on the executors */
    DriverRegistry.register(VectorJDBC.DriverClassName)

    task.addTaskCompletionListener { _ =>
      close(rs, "result set")
      close(stmt, "statement")
      close(cxn, "vector JDBC connection")
      closed = true
      if (it != null) it.profilePrint(it.profAccs)
    }

    logDebug(s"Computing partition in ScanRDD by issuing JDBC select statement: $vectorSelectStatement")
    lazy val cxn = new VectorJDBC(connProps)
    lazy val (stmt, rs) = cxn.executeUnmanagedPreparedQuery(vectorSelectStatement, vectorSelectParams)
    it = new ResultSetRowIterator(rs)
    it
  }

  private def close[T <: { def close() }](c: T, resourceName: String): Unit = if (!closed && c != null) {
    try { c.close } catch { case e: Exception => logWarning(s"Exception closing $resourceName", e) }
  }
}
