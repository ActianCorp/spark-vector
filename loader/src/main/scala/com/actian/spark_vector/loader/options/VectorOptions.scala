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
package com.actian.spark_vector.loader.options

import com.actian.spark_vector.vector.VectorConnectionProperties

case class VectorOptions(
  host: String = "",
  instance: String = "",
  database: String = "",
  user: Option[String] = None,
  password: Option[String] = None,
  port: String = "7",
  targetTable: String = "",
  preSQL: Option[Seq[String]] = None,
  postSQL: Option[Seq[String]] = None)
  
object VectorOptions {
  def getConnectionProps(vo: VectorOptions): VectorConnectionProperties =
    VectorConnectionProperties(vo.host,  vo.instance, vo.database, vo.user, vo.password, vo.port)

}
