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
import com.actian.spark_vector.vector.PredicatePushdown.ValueRanges

import play.api.libs.json.{ Format, JsObject, Json, Reads, Writes }

private[provider] case class LogicalType(`type`: String,
  precision: Int,
  scale: Int)

private[provider] case class ColumnInfo(column_name: String,
    logical_type: LogicalType,
    physical_type: String,
    nullable: Boolean,
    value_ranges: Option[ValueRanges]) {
  implicit def toColumnMetadata: ColumnMetadata = ColumnMetadata(column_name, logical_type.`type`, nullable, logical_type.precision, logical_type.scale, value_ranges)
}

/** How many streams does `Vector` expect on the node with host name `host`, at `port` */
private[provider] case class StreamPerNode(nr: Int,
  port: Int,
  host: String)

/**
 * Metadata about datastreams.
 *
 * @param rolename The username to use when authenticating with `Vector`'s datastreams
 * @param password The password to use when authenticating with `Vector`'s datastreams
 * @param streams_per_node For each node that expects `Datastream` connections, this sequence will contain a `StreamPerNode` element
 */
private[provider] case class DataStream(rolename: String,
  password: String,
  streams_per_node: Seq[StreamPerNode])

/**
 * Part of a Vector external table request.
 *
 * @param part_id Unique identifier for this part of the request
 * @param external_table_name The name of the external table, as defined in Vector's SQL
 * @param operator_type The type of operation mapped to this request part. It can either be 'scan' or 'insert'.
 * @param external_reference The reference to the external data source, e.g. file path for file sources, table names for database sources, etc.
 * @param format What is the format of the external datasource
 * @param column_infos Information about the columns read/inserted in the external datasource.
 * @param options Additional options that need to be provided to the datasource
 * @param datastream Connection (to `Vector`'s datastreams) information
 */
private[provider] case class JobPart(part_id: String,
    external_table_name: String,
    operator_type: String,
    external_reference: String,
    format: Option[String],
    column_infos: Seq[ColumnInfo],
    options: Option[Map[String, String]],
    datastream: DataStream) {
  def conf: VectorEndpointConf = {
    val endpoints = for {
      spn <- datastream.streams_per_node
      i <- 0 until spn.nr
    } yield VectorEndpoint(spn.host, spn.port, datastream.rolename, datastream.password)
    VectorEndpointConf(endpoints.toIndexedSeq)
  }
}

private[provider] object JobPart {
  implicit val logicalTypeFormat = Json.format[LogicalType]
  implicit val columnInfoFormat = Json.format[ColumnInfo]
  implicit val streamPerNodeFormat = Json.format[StreamPerNode]
  implicit val dataStreamFormat = Json.format[DataStream]
  implicit val jobPartFormat = Json.format[JobPart]
}

/**
 * A query request for this provider.
 *
 * @note Each `Job` has multiple [[parts]] that are executed sequentially
 */
private[provider] case class Job(transaction_id: Long, query_id: Long, `type`: String, parts: Seq[JobPart])

private[provider] object Job {
  implicit val jobFormat = Json.format[Job]
}

private[provider] sealed trait JobStatus

private[provider] sealed trait JobSuccess extends JobStatus
private[provider] case object JobSuccess extends JobSuccess {
  implicit val successFormat = Format[JobSuccess](Reads { _.validate[JsObject] map (_ => JobSuccess) }, Writes { _ => JsObject(Nil) })
}

private[provider] sealed trait JobAck extends JobStatus
private[provider] case object JobAck extends JobAck {
  implicit val ackFormat = Format[JobAck](Reads { _.validate[JsObject] map (_ => JobAck) }, Writes { _ => JsObject(Nil) })
}

/**
 * A job message represents some kind of feedback from the provider
 *
 * @param part_id Identifier for the part that generated this message
 * @param code A code for the message in case
 * @param msg The detailed message
 * @param stacktrace Stack trace of the point in the code where the message was generated
 */
private[provider] case class JobMsg(part_id: Option[String] = None, code: Option[Int] = None, msg: String, stacktrace: Option[String])

/**
 * Profile information for the query request
 *
 * @param stage The name of the portion of the execution that has its profile information stored in this structure
 * @param time Time in microseconds spend in this [[stage]]
 */
private[provider] case class JobProfile(part_id: String, stage: String, time: Long)

/**
 * The result of a query request
 *
 * @note only one of [[ack]]/[[success]]/[[error]] should be set
 *
 * @param error A list of error messages generated by this query (if any)
 * @param warn A list of warning messages generated by this query
 * @param profile Profiling information generated by this query
 */
private[provider] case class JobResult(transaction_id: Long,
  query_id: Long,
  success: Option[JobSuccess] = None,
  ack: Option[JobAck] = None,
  error: Option[Seq[JobMsg]] = None,
  warn: Option[Seq[JobMsg]] = None,
  profile: Option[Seq[JobProfile]] = None)

private[provider] object JobResult {
  implicit val msgFormat = Json.format[JobMsg]
  implicit val profileFormat = Json.format[JobProfile]
  implicit val jobResultFormat = Json.format[JobResult]
}

/** Wrap an exception in a class that contains additional information about the `Job` that has thrown this exception */
case class JobException(cause: Throwable, job: Job, part: JobPart) extends Throwable(cause)
