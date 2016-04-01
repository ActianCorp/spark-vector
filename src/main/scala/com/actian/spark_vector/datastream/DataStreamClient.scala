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
 */
case class DataStreamClient(vectorProps: VectorConnectionProperties, table: String) extends Serializable with Logging {
  private lazy val jdbc = {
    val ret = new VectorJDBC(vectorProps)
    ret.autoCommit(false)
    ret
  }

  private def prepareLoadSql(table: String) = s"prepare for x100 stream into $table"
  private def startLoadSql(table: String) = s"copy table $table from external"

  private def prepareUnloadSql(table: String) = s"prepare for x100 stream from $table"
  private def startUnloadSql(selectStatement: String) = s"insert into external table $selectStatement"

  private def startFutureQuery(query: String, whereParams: Seq[Any] = Seq.empty[Any]): Future[Int] = Future {
    val ret = try {
      closeResourceOnFailure(this) {
        jdbc.executePreparedQuery(query, whereParams)((rs: ResultSet) => {
          val rsLast = rs.last()
          val rsNumRows = rs.getRow()
          rs.close()
          rsNumRows
        })
      }
    } catch {
      case e: SQLException =>
        throw new VectorException(e.getErrorCode, e.getMessage, e)
    }
    ret
  }

  /** The `JDBC` connection used by this client to communicate with `Vector` */
  def getJdbc(): VectorJDBC = jdbc

  /** Abort sending data to Vector rolling back the open transaction and closing the `JDBC` connection */
  def close(): Unit = {
    logDebug("Closing DataStreamClient")
    jdbc.rollback
    jdbc.close
  }

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
  def startLoad(): Future[Int] = startFutureQuery(startLoadSql(table))

  /** Start unloading data from Vector */
  def startUnload(selectStatement: String, whereParams: Seq[Any]): Future[Int] =
    startFutureQuery(startUnloadSql(selectStatement), whereParams)

  /** Commit the transaction opened by this client */
  def commit: Unit = jdbc.commit
}
