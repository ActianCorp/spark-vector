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
package com.actian.spark_vector.sql

import com.actian.spark_vector.vector.VectorConnectionProperties

/** A reference to a `Vector` table */
case class TableRef(host: String, instance: String, database: String, user: Option[String], password: Option[String], table: String, cols: Seq[String]) {
  def toConnectionProps: VectorConnectionProperties = VectorConnectionProperties(host, instance, database, user, password)
}

object TableRef {
  def apply(parameters: Map[String, String]): TableRef = {
    val host = parameters("host")
    val instance = parameters("instance")
    val database = parameters("database")
    val table = parameters("table")
    val user = if (parameters.contains("user")) Some(parameters("user")) else None
    val password = if (parameters.contains("password")) Some(parameters("password")) else None
    val colsToLoad = parameters.get("cols").map(_.split(",").map(_.trim).toSeq).getOrElse(Nil)
    TableRef(host, instance, database, user, password, table, colsToLoad)
  }

  def apply(connectionProps: VectorConnectionProperties, table: String): TableRef = TableRef(connectionProps.host,
    connectionProps.instance, connectionProps.database, connectionProps.user, connectionProps.password, table, Nil)
}
