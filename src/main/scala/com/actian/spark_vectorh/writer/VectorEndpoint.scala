package com.actian.spark_vectorh.writer

import scala.util.Try

import org.apache.spark.Logging

import com.actian.spark_vectorh.vector.VectorJDBC

/** Information to connect to a VectorEndpoint (DataStream) */
case class VectorEndPoint(host: String,
  port: Int,
  username: String,
  password: String) extends Serializable

/**
 * Contains helpers to obtain VectorEndpoint information from `Vector(H)`'s SQL interface.
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
