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
package com.actian.spark_vector.loader.command

import org.apache.spark.sql.SQLContext

import com.actian.spark_vector.loader.options.UserOptions

object VectorTempTable {
  private def parseOptions(config: UserOptions): String = {
    val basic = Seq(s"""host "${config.vector.host}"""",
      s"""instance "${config.vector.instance}"""",
      s"""database "${config.vector.database}"""",
      s"""table "${config.vector.targetTable}"""")
    val optional = Seq(config.vector.user.map(user => s"""user "${user}""""),
      config.vector.password.map(pass => s"""password "${pass}"""")).flatten
    (basic ++ optional).mkString(",")
  }

  /**
   * Based on `config`, register a temporary table as the source of the `Vector` table being loaded to
   *
   * @return The name of the registered temporary table (for now = <vectorTargetTable>)
   */
  def register(config: UserOptions, sqlContext: SQLContext): String = {
    val table = config.vector.targetTable
    sqlContext.sql(s"""CREATE TEMPORARY TABLE ${table}
    USING com.actian.spark_vector.sql.DefaultSource
    OPTIONS (${parseOptions(config)})""")
    table
  }
}
