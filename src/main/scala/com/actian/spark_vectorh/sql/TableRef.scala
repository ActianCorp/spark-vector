package com.actian.spark_vectorh.sql

import com.actian.spark_vectorh.vector.VectorConnectionProperties

case class TableRef(host: String, instance: String, database: String, user: Option[String], password: Option[String], table: String) {
  def toConnectionProps = {
    VectorConnectionProperties (host, instance, database, user, password)
  }
}

