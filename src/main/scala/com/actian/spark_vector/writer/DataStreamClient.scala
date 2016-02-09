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

import java.sql.SQLException
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
case class DataStreamClient(vectorProps: VectorConnectionProperties,
  table: String) extends Serializable with Logging {
  private lazy val jdbc = {
    val ret = new VectorJDBC(vectorProps)
    ret.autoCommit(false)
    ret
  }

  private def startLoadSql(table: String) = s"copy table $table from external"
  private def prepareLoadSql(table: String) = s"prepare for x100 copy into $table"

  /** The `JDBC` connection used by this client to communicate with `Vector(H)` */
  def getJdbc(): VectorJDBC = jdbc

  /** Abort sending data to Vector(H) rolling back the open transaction and closing the `JDBC` connection */
  def close(): Unit = {
    logDebug("Closing DataStreamClient")
    jdbc.rollback
    jdbc.close
  }

  /** Prepare loading data to Vector(H). This step may not be necessary anymore in the future */
  def prepareDataStreams: Unit = jdbc.executeStatement(prepareLoadSql(table))

  /**
   * Obtain the information about how many `DataStream`s Vector(H) expects together with
   * locality information and authentication roles and tokens
   */
  def getWriteConf(): WriteConf = {
    val ret = WriteConf(jdbc)
    logDebug(s"Got ${ret.vectorEndPoints.length} datastreams")
    ret
  }

  /** Start loading data to Vector(H) */
  def startLoad(): Future[Int] = Future {
    val ret = try {
      closeResourceOnFailure(this) {
        jdbc.executeStatement(startLoadSql(table))
      }
    } catch {
      case e: SQLException =>
        throw new VectorException(e.getErrorCode, e.getMessage, e)
    }
    ret
  }

  /** Commit the transaction opened by this client */
  def commit: Unit = jdbc.commit
}
