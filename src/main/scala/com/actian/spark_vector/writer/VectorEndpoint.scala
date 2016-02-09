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

import scala.util.Try

import org.apache.spark.Logging

import com.actian.spark_vector.vector.VectorJDBC

/** Information to connect to a VectorEndpoint (DataStream) */
case class VectorEndPoint(host: String,
  port: Int,
  username: String,
  password: String) extends Serializable

/**
 * Contains helpers to obtain VectorEndpoint information from `Vector`'s SQL interface.
 *
 * @note The way this information is obtained, by issuing a select from a system table, will very likely be modified in the future
 */
object VectorEndPoint extends Logging {
  private val hostDbColumn = "host"
  private val portDbColumn = "port"
  private val usernameDbColumn = "username"
  private val passwordDbColumn = "password"

  private val dataStreamsTable = "iivwtable_datastreams"

  private val getVectorEndPointSql: String = s"select $hostDbColumn, $portDbColumn, $usernameDbColumn, $passwordDbColumn from $dataStreamsTable"

  def apply(seq: Seq[Any], jdbcHost: String = "localhost"): Option[VectorEndPoint] = {
    seq match {
      case Seq(host: String, port: String, username: String, password: String) =>
        Try {
          val real_host = host match {
            case "" => jdbcHost
            case _ => host
          }
          VectorEndPoint(real_host.toUpperCase, port.toInt, username, password)
        }.toOption
      case _ => None
    }
  }

  /** Issues a query through JDBC to obtain connection information from the `DataStreams` system table */
  def fromDataStreamsTable(cxn: VectorJDBC): IndexedSeq[VectorEndPoint] = {
    val resultSet = cxn.query(getVectorEndPointSql)
    val ret = resultSet
      .map(VectorEndPoint(_, cxn.getIngresHost))
      .flatten
    logDebug(s"Got the following VectorEndPoints from the datastreams table: ${ret.map(_.toString).mkString(",")}")
    ret.toIndexedSeq
  }
}
