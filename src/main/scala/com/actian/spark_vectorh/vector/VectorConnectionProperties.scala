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
package com.actian.spark_vectorh.vector

/**
 * Container for VectorH connection properties.
 */
case class VectorConnectionProperties(host: String,
  instance: String,
  database: String,
  user: Option[String] = None,
  password: Option[String] = None) extends Serializable {
  require(host != null && host.length > 0, "The host property is required and cannot be null or empty")
  require(instance != null && instance.length > 0, "The instance property is required and cannot be null or empty")
  require(database != null && database.length > 0, "The database property is required and cannot be null or empty")

  def toJdbcUrl: String = {
    s"jdbc:ingres://${host}:${instance}7/${database}"
  }
}
