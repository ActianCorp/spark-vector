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

import java.sql.SQLException
import java.sql.ResultSet

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark.Logging

import com.actian.spark_vector.util.ResourceUtil.closeResourceOnFailure
import com.actian.spark_vector.vector.{ VectorConnectionProperties, VectorJDBC }
import com.actian.spark_vector.vector.VectorException
import com.actian.spark_vector.vector.ErrorCodes

/**
 * A client to prepare loading and issue the load `SQL` query to Vector
 *
 * @param vectorProps connection information
 * @param table to which table this client will load data
 *
 * @note This client opens a JDBC connection when instantiated. To prevent leaks,
 * the [[close]] method must be called
 */
case class DataStreamClient(vectorProps: VectorConnectionProperties, table: String) extends Serializable with Logging {
  private val jdbc = {
    val ret = new VectorJDBC(vectorProps)
    ret.autoCommit(false)
    ret
  }

  private def prepareLoadSql(table: String) = s"prepare for x100 stream into $table"
  private def startLoadSql(table: String) = s"copy table $table from external"

  private def prepareUnloadSql(table: String) = s"prepare for x100 stream from $table"
  private def startUnloadSql(selectQuery: String) = s"insert into external table $selectQuery"

  /** Execute a sql (statement) within a future task */
  private def executeSql(sql: String, params: Seq[Any] = Nil): Future[Int] = {
    val f = Future { if (params.isEmpty) jdbc.executeStatement(sql) else jdbc.executePreparedStatement(sql, params) }
    f onFailure {
      case t =>
        logError(s"Query ${sql} has failed.", t)
        // FIXME: use rollback() instead when Vector will actually close the DataStreams after unrolling
        close()
    }
    f
  }

  /** The `JDBC` connection used by this client to communicate with `Vector` */
  def getJdbc(): VectorJDBC = jdbc

  /**
   * Abort sending data to Vector rolling back the open transaction
   * @note the `JDBC` connection is not closed
   */
  def rollback(): Unit = synchronized(if (!jdbc.isClosed) {
    logDebug("Rollback current transaction")
    jdbc.rollback
  })

  /** Abort sending data to Vector rolling back the open transaction and closing the `JDBC` connection */
  def close(): Unit = synchronized(if (!jdbc.isClosed) {
    logDebug("Rollback current transaction and close DataStreamClient")
    jdbc.rollback
    jdbc.close
  })

  /** Prepare loading/unloading data to Vector. These steps may not be necessary anymore in the future */
  def prepareLoadDataStreams: Unit = jdbc.executeStatement(prepareLoadSql(table))
  def prepareUnloadDataStreams: Unit = jdbc.executeStatement(prepareUnloadSql(table))

  /**
   * Obtain the information about how many `DataStream`s Vector expects together with
   * locality information and authentication roles and tokens
   */
  def getVectorEndpointConf(): VectorEndpointConf = {
    val ret = VectorEndpointConf(jdbc)
    logDebug(s"Got ${ret.vectorEndpoints.length} datastreams")
    ret
  }

  /** Start loading data to Vector */
  def startLoad(): Future[Int] = executeSql(startLoadSql(table))

  /** Start unloading data from Vector */
  def startUnload(selectQuery: String, whereParams: Seq[Any]): Future[Int] = executeSql(startUnloadSql(selectQuery), whereParams)

  /** Commit the transaction opened by this client */
  def commit: Unit = jdbc.commit
}
