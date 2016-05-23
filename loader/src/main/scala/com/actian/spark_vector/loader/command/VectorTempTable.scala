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
import com.actian.spark_vector.sql.{ TableRef, VectorRelation }

object VectorTempTable {
  private def generateSQLOption(key: String, queries: Seq[String]): Seq[(String, String)] = for { i <- 0 until queries.size } yield {
    (s"${key}${i}", s"${queries(i)}")
  }

  private def parseOptions(config: UserOptions): Map[String, String] = {
    val base = Seq("host" -> config.vector.host,
      "instance" -> config.vector.instance,
      "database" -> config.vector.database,
      "table" -> config.vector.targetTable)
    val optional = Seq(config.vector.user.map("user" -> _),
      config.vector.password.map("password" -> _)).flatten ++
      config.vector.preSQL.map(generateSQLOption("loadpresql", _)).getOrElse(Nil) ++
      config.vector.postSQL.map(generateSQLOption("loadpostsql", _)).getOrElse(Nil)
    (base ++ optional).toMap
  }

  /** Based on `config`, register a temporary table as the source of the `Vector` table being loaded to
   *
   *  @return The name of the registered temporary table (for now = <vectorTargetTable>)
   */
  def register(config: UserOptions, sqlContext: SQLContext): String = {
    val params = parseOptions(config)
    val tableName = params("table")
    sqlContext.baseRelationToDataFrame(VectorRelation(TableRef(params), sqlContext, Map.empty[String, String])).registerTempTable(tableName)
    sparkQuote(tableName)
  }
}
