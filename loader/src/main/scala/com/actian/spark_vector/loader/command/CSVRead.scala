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
import com.actian.spark_vector.sql.sparkQuote
import com.actian.spark_vector.util.Logging

object CSVRead extends Logging {
  private def parseOptions(options: UserOptions): String = {
    Seq(
      Some(s"""sep "${options.csv.separatorChar.getOrElse(",")}""""),
      options.csv.headerRow.filter(identity).map(_ => """header "true""""),
      options.csv.inferSchema.filter(identity).map(_ => s"""inferSchema "true""""),
      options.csv.encoding.map(v => s"""encoding "${v}""""),
      options.csv.quoteChar.map(c => if (c != '\'') s"quote '$c'" else s"""quote "$c""""),
      options.csv.escapeChar.map(c => if (c != '\'') s"""escape '$c'""" else s"""escape "$c""""),
      options.csv.commentChar.map(c => if (c != '\'') s"comment '$c'" else s"""comment "$c""""),
      options.csv.ignoreLeading.filter(identity).map(_ => """ignoreLeadingWhiteSpace "true""""),
      options.csv.ignoreTrailing.filter(identity).map(_ => """ignoreTrailingWhiteSpace "true""""),
      options.csv.nullValue.map(v => s"""nullValue "${v}""""),
      options.csv.nanValue.map(v => s"""nanValue "${v}""""),
      options.csv.positiveInf.map(v => s"""positiveInf "${v}""""),
      options.csv.negativeInf.map(v => s"""negativeInf "${v}""""),
      options.csv.dateFormat.map(v => s"""dateFormat "${v}""""),
      options.csv.timestampFormat.map(v => s"""timestampFormat "${v}""""),
      options.csv.parseMode.map(_.toUpperCase()).collect {
        case "PERMISSIVE" => """mode "PERMISSIVE" """
        case "DROPMALFORMED" => """mode "DROPMALFORMED""""
        case "FAILFAST" => """mode "FAILFAST""""
      }
      ).flatten.mkString(",", ",", "")
  }

  /**
   * Based on `options`, register a temporary table as the source of the `CSV` input with the appropriate options introduced
   * as required by `spark-csv`.
   *
   * @return A string containing the `SELECT` statement that can be used to subsequently consume data from the temporary
   * table
   * @note The temporary table will be named "csv_<vectorTargetTable>*"
   */
  def registerTempTable(options: UserOptions, sqlContext: SQLContext): String = {
    val table = s"csv_${options.vector.targetTable}_${System.currentTimeMillis}"
    val quotedTable = sparkQuote(table)
    val baseQuery = s"""CREATE TEMPORARY VIEW $quotedTable${options.csv.header.map(_.mkString("(", ",", ")")).getOrElse("")}
      USING csv
      OPTIONS (path "${options.general.sourceFile}"${parseOptions(options)})"""
    logDebug(s"CSV query to be executed for registering temporary table:\n$baseQuery")
    val df = sqlContext.sql(baseQuery)
    val cols = options.general.colsToLoad.getOrElse(sqlContext.sql(s"select * from $quotedTable where 1=0").columns.toSeq)
    s"select ${cols.map(c => s"""`${c.trim}`""").mkString(",")} from $quotedTable"
  }
}
