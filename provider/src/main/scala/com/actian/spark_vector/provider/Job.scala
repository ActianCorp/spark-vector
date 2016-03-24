package com.actian.spark_vector.provider

import com.actian.spark_vector.writer.VectorEndPoint
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

sealed case class JobPart(part_id: String,
  vector_table_name: String,
  spark_sql_query: String,
  cols_to_load: Option[Seq[String]],
  spark_ref: String,
  options: Map[String, String],
  datastreams: IndexedSeq[VectorEndPoint])

object JobPart {
  implicit lazy val vectorEndPointReads: Reads[VectorEndPoint] = (
    (JsPath \ "host").read[String] and
    (JsPath \ "port").read[Int] and
    (JsPath \ "username").read[String] and
    (JsPath \ "password").read[String])(VectorEndPoint.apply(_: String, _: Int, _: String, _: String))

  implicit lazy val vectorEndPointWrites: Writes[VectorEndPoint] = (
    (JsPath \ "host").write[String] and
    (JsPath \ "port").write[Int] and
    (JsPath \ "username").write[String] and
    (JsPath \ "password").write[String])(unlift(VectorEndPoint.unapply))

  implicit val jobPartFormat = Json.format[JobPart]
}

case class Job(query_id: String, job_parts: Seq[JobPart])

object Job {
  import JobPart._
  implicit val jobFormat = Json.format[Job]
}