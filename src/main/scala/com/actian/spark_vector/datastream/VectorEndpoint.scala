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

import scala.util.Try

import com.actian.spark_vector.util.Logging
import com.actian.spark_vector.vector.VectorJDBC
import com.actian.spark_vector.vector.ColumnMetadata

/** Information to connect to a VectorEndpoint (DataStream) */
case class VectorEndpoint(host: String, port: Int, username: String, password: String) extends Serializable

/**
 * Contains helpers to obtain VectorEndpoint information from `Vector`'s SQL interface.
 *
 * @note The way this information is obtained, by issuing a select from a system table, will very likely be modified in the future
 */
object VectorEndpoint extends Logging {
  private val hostDbColumn = "host"
  private val qhostDbColumn = "qhost"
  private val portDbColumn = "port"
  private val usernameDbColumn = "username"
  private val passwordDbColumn = "password"
  private val dataStreamsTable = "iivwtable_datastreams"

  def apply(seq: Seq[Any], jdbcHost: String = "localhost"): Option[VectorEndpoint] = seq match {
    case Seq(host: String, port: String, username: String, password: String) =>
      Try {
        val real_host = host match {
          case "" => jdbcHost
          case _ => host
        }
        VectorEndpoint(real_host, port.toInt, username, password)
      }.toOption
    case _ => None
  }

  /** If possible, we try to use the fully qualified hostname (qhost) instead of the
   * simple hostname (host) in order to avoid issues in a Kubernetes setup. However,
   * depending on the VH version, the qhost column might not be available yet as it
   * was first introduced with VH 6.1.0.
   */
  private def extractHostColumnName(col_meta: Seq[ColumnMetadata]): String = {
    val res = col_meta.filter(col => { col.name == qhostDbColumn || col.name == hostDbColumn})
    res match {
        case Seq(_, ColumnMetadata(`qhostDbColumn`,_,_,_,_,_)) => qhostDbColumn
        case Seq(ColumnMetadata(`qhostDbColumn`,_,_,_,_,_), _*) => qhostDbColumn
        case Seq(ColumnMetadata(`hostDbColumn`,_,_,_,_,_)) => hostDbColumn
        case _ => throw new IllegalStateException(s"Table $dataStreamsTable is missing a host column!")
    }
  }

  /** Issues a query through JDBC to obtain connection information from the `DataStreams` system table */
  def fromDataStreamsTable(cxn: VectorJDBC): IndexedSeq[VectorEndpoint] = {
    val col_meta = cxn.columnMetadata(dataStreamsTable)
    val getVectorEndPointSql: String = s"select ${extractHostColumnName(col_meta)}, $portDbColumn, $usernameDbColumn, $passwordDbColumn from $dataStreamsTable"
    logDebug(s"Running sql query ${getVectorEndPointSql} to get the datastream endpoints' info.")
    val resultSet = cxn.query(getVectorEndPointSql)
    val ret = resultSet
      .map(VectorEndpoint(_, cxn.getIngresHost))
      .flatten
    logDebug(s"Got the following VectorEndPoints from the datastreams table: ${ret.map(_.toString).mkString(",")}")
    ret.toIndexedSeq
  }
}
