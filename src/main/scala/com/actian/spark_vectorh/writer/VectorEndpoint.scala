package com.actian.spark_vectorh.writer

import scala.util.Try

import org.apache.spark.Logging

import com.actian.spark_vectorh.vector.VectorJDBC

case class VectorEndPoint(host: String,
  port: Int,
  username: String,
  password: String) extends Serializable

object VectorEndPoint extends Logging {
  val hostDbColumn = "host"
  val portDbColumn = "port"
  val usernameDbColumn = "username"
  val passwordDbColumn = "password"

  val dataStreamsTable = "iivwtable_datastreams"

  def getVectorEndPointSql: String = s"select $hostDbColumn, $portDbColumn, $usernameDbColumn, $passwordDbColumn from $dataStreamsTable"

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

  def fromDataStreamsTable(cxn: VectorJDBC): IndexedSeq[VectorEndPoint] = {
    val resultSet = cxn.query(getVectorEndPointSql)
    val ret = resultSet
      .map(VectorEndPoint(_, cxn.getIngresHost))
      .flatten
    log.debug(s"Got the following VectorEndPoints from the datastreams table: ${ret.map(_.toString).mkString(",")}")
    ret.toIndexedSeq
  }
}
