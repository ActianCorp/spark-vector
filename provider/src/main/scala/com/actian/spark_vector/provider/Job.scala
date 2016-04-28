package com.actian.spark_vector.provider

import com.actian.spark_vector.writer.VectorEndPoint
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import com.actian.spark_vector.vector.ColumnMetadata
import com.actian.spark_vector.writer.WriteConf

private[provider] case class LogicalType(`type`: String,
  precision: Int,
  scale: Int)

private[provider] case class ColumnInfo(column_name: String,
    logical_type: LogicalType,
    physical_type: String,
    nullable: Boolean,
    is_const: Boolean) {
  implicit def toColumnMetadata: ColumnMetadata = ColumnMetadata(column_name, logical_type.`type`, nullable, logical_type.precision, logical_type.scale)
}

private[provider] case class StreamPerNode(nr: Int,
    port: Int,
    host: String) {
  private val ReasonableThreadsPerNode = 1024
  require(nr <= ReasonableThreadsPerNode)
}

private[provider] case class DataStream(
  rolename: String,
  password: String,
  streams_per_node: Seq[StreamPerNode])

private[provider] case class JobPart(part_id: String,
    external_table_name: String,
    operator_type: String,
    external_reference: String,
    format: Option[String],
    column_infos: Seq[ColumnInfo],
    options: Map[String, String],
    datastream: DataStream) {
  def writeConf: WriteConf = {
    val endpoints = for {
      spn <- datastream.streams_per_node
      i <- 0 until spn.nr
    } yield VectorEndPoint(spn.host, spn.port, datastream.rolename, datastream.password)
    WriteConf(endpoints.toIndexedSeq)
  }
}

object JobPart {
  implicit val logicalTypeFormat = Json.format[LogicalType]
  implicit val columnInfoFormat = Json.format[ColumnInfo]
  implicit val streamPerNodeFormat = Json.format[StreamPerNode]
  implicit val dataStreamFormat = Json.format[DataStream]
  implicit val jobPartFormat = Json.format[JobPart]
}

case class Job(transaction_id: Long, query_id: Long, `type`: String, parts: Seq[JobPart])

object Job {
  import JobPart._
  implicit val jobFormat = Json.format[Job]
  final val QueryId = "query_id"
  final val JobParts = "job_parts"
  final val PartId = "part_id"
  final val VectorTableName = "vector_table_name"
  final val SqlQuery = "spark_sql_query"
  final val ColsToLoad = "cols_to_load"
  final val Ref = "spark_ref"
  final val Options = "options"
  final val Format = "format"
  final val DataStreams = "datastreams"
  final val DataStreamHost = "host"
  final val DataStreamPort = "port"
  final val UserName = "username"
  final val Password = "password"
}
