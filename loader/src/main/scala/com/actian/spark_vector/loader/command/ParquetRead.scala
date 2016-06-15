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
import com.actian.spark_vector.sql.{ sparkQuote, colsSelectStatement }

object ParquetRead {
  /**
   * Based on `options`, register a temporary table as the source of the `Parquet` input
   *
   * @return A string containing the `SELECT` statement that can be used to subsequently consume data from the temporary table
   * @note The temporary table will be named "parquet_<vectorTargetTable>*"
   */
  def registerTempTable(options: UserOptions, sqlContext: SQLContext): String = {
    val table = s"parquet_${options.vector.targetTable}_${System.currentTimeMillis}"
    sqlContext.read.parquet(options.general.sourceFile).registerTempTable(table)
    s"select ${colsSelectStatement(options.general.colsToLoad)} from ${sparkQuote(table)}"
  }
}
