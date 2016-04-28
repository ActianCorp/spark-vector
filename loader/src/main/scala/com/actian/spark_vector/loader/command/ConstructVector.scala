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

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

import com.actian.spark_vector.loader.options.UserOptions
import com.actian.spark_vector.loader.parsers.Args

object ConstructVector {
  /**
   * Based on the options parsed from the command line, define the main body of the `Spark` job to be submitted
   *
   * @param config User options parsed from the command line
   */
  def execute(config: UserOptions): Unit = {
    val conf = new SparkConf()
      .setAppName(s"Spark-Vector ${config.mode} load into ${config.vector.targetTable}")
      .set("spark.task.maxFailures", "1")
    val sparkContext = new SparkContext(conf)
    val sqlContext = config.mode match {
      case Args.orcLoad.longName => new HiveContext(sparkContext)
      case _ => new SQLContext(sparkContext)
    }

    val select = config.mode match {
      case Args.csvLoad.longName => CSVRead.registerTempTable(config, sqlContext)
      case Args.parquetLoad.longName => ParquetRead.registerTempTable(config, sqlContext)
      case Args.orcLoad.longName => OrcRead.registerTempTable(config, sqlContext)
      case m => throw new IllegalArgumentException(s"Invalid configuration mode: ${m}")
    }

    val targetTempTable = VectorTempTable.register(config, sqlContext)

    // Load the line item data into Vector
    sqlContext.sql(s"insert into table ${targetTempTable} ${select}")
  }
}
