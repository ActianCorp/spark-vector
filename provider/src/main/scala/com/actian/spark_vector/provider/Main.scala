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
package com.actian.spark_vector.provider

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.actian.spark_vector.loader.command.VectorTempTable
import com.actian.spark_vector.writer.VectorEndPoint
import org.apache.spark.Logging
import com.actian.spark_vector.writer.WriteConf
import com.actian.spark_vector.loader.command._
import org.apache.spark.sql.DataFrame
import com.actian.spark_vector.sql.VectorRelation
import com.actian.spark_vector.sql.TableRef
import play.api.libs.json._
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException

object Main extends App with Logging {
  private val conf = new SparkConf()
    .setAppName("Spark-Vector external tables provider")
    .set("spark.task.maxFailures", "1")
  private val sc = new SparkContext(conf)
  private val sqlContext = new HiveContext(sc)

  private lazy val vectorHost = "schwalbe"
  private lazy val vectorInstance = "C2"
  private lazy val vectorDatabase = "test"
  private lazy val connectionParams = Map("host" -> vectorHost, "instance" -> vectorInstance, "database" -> vectorDatabase)

  private lazy val handler = new RequestHandler(sqlContext, connectionParams)

  @inline def finished(line: String): Boolean = {
    line == null || line == "<END>"
  }

  logInfo("Spark-Vector provider initialized and starting listening for requests...")

  Iterator.continually(Console.readLine).takeWhile(!finished(_)).foreach { line =>
    handler.handle(line)
  }
}
