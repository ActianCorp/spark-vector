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
package com.actian.spark_vector.provider

import com.actian.spark_vector.datastream.{ VectorEndpoint, VectorEndpointConf }
import com.actian.spark_vector.vector.ColumnMetadata

import play.api.libs.json.Json

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
    options: Option[Map[String, String]],
    datastream: DataStream) {
  def writeConf: VectorEndpointConf = {
    val endpoints = for {
      spn <- datastream.streams_per_node
      i <- 0 until spn.nr
    } yield VectorEndpoint(spn.host, spn.port, datastream.rolename, datastream.password)
    VectorEndpointConf(endpoints.toIndexedSeq)
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
  implicit val jobFormat = Json.format[Job]
}

case class JobMsg(part_id: String, code: Int, msg: String, stacktrace: Option[String])

case class JobProfile(part_id: String, stage: String, time: Long)

case class JobResult(transaction_id: Long, query_id: Long, success: Option[Boolean] = None, error: Option[Seq[JobMsg]] = None, warn: Option[Seq[JobMsg]] = None, profile: Option[Seq[JobProfile]] = None)

object JobResult {
  implicit val msgFormat = Json.format[JobMsg]
  implicit val profileFormat = Json.format[JobProfile]
  implicit val jobResultFormat = Json.format[JobResult]
}
