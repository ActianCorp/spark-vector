package com.actian.spark_vectorh.sql

import com.actian.spark_vectorh.vector.VectorConnectionProperties

/** A reference to a `Vector(H)` table */
case class TableRef(host: String, instance: String, database: String, user: Option[String], password: Option[String], table: String) {
  def toConnectionProps = {
    VectorConnectionProperties(host, instance, database, user, password)
  }
}

object TableRef {
  def apply(parameters: Map[String, String]): TableRef = {
    val host = parameters("host")
    val instance = parameters("instance")
    val database = parameters("database")
    val table = parameters("table")
    val user =
      if (parameters.contains("user")) Some(parameters("user"))
      else None
    val password =
      if (parameters.contains("password")) Some(parameters("password"))
      else None
    TableRef(host, instance, database, user, password, table)
  }
}
