/*
 * Copyright 2019 Actian Corporation
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
import com.actian.spark_vector.sql.sparkQuote
import com.actian.spark_vector.util.Logging
import com.actian.spark_vector.sql.colsSelectStatement

object JSONRead extends Logging { 
 
  private def parseOptions(options: UserOptions): String = {
    Seq(
      options.json.primitivesAsString.filter(identity).map(_ => """primitivesAsString "true""""),
      options.json.allowComments.filter(identity).map(_ => """allowComments "true""""),
      options.json.allowUnquoted.filter(identity).map(_ => """allowUnquotedFieldNames "true""""),
      options.json.allowSingleQuotes.filter(identity).map(_ => """allowSingleQuotes "true""""),
      options.json.allowLeadingZeros.filter(identity).map(_ => """allowNumericLeadingZeros "true""""),
      options.json.allowEscapingAny.filter(identity).map(_ => """allowBackslashEscapingAnyCharacter "true""""),
      options.json.allowUnquotedControlChars.filter(identity).map(_ => """allowUnquotedControlChars "true""""),
      options.json.multiline.filter(identity).map(_ => """multiline "true""""),
      options.json.parseMode.map(_.toUpperCase()).collect {
        case "PERMISSIVE" => """mode "PERMISSIVE" """
        case "DROPMALFORMED" => """mode "DROPMALFORMED""""
        case "FAILFAST" => """mode "FAILFAST""""
      }
    ).flatten.mkString(",", ",", "")
  }  
  
  /**
   * Based on `options`, register a temporary table as the source of the `JSON` input with the appropriate options introduced
   * as required by `spark-json`.
   *
   * @return A string containing the `SELECT` statement that can be used to subsequently consume data from the temporary
   * table
   * @note The temporary table will be named "json_<vectorTargetTable>*"
   */
  def registerTempTable(options: UserOptions, sqlContext: SQLContext): String = {
      val table = s"json_${options.vector.targetTable}_${System.currentTimeMillis}"
      val quotedTable = sparkQuote(table)
      val baseQuery = s"""CREATE TEMPORARY VIEW $quotedTable${options.json.header.map(_.mkString("(", ",", ")")).getOrElse("")}
      USING json
      OPTIONS (path "${options.general.sourceFile}"${parseOptions(options)})"""
      logDebug(s"JSON query to be executed for registering temporary table:\n$baseQuery")
      val df = sqlContext.sql(baseQuery)
      val cols = options.general.colsToLoad.getOrElse(sqlContext.sql(s"select * from $quotedTable where 1=0").columns.toSeq)
      s"select ${cols.map(c => s"""`${c.trim}`""").mkString(",")} from $quotedTable"    
   }
}

