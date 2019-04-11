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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.actian.spark_vector.vector.VectorOps._
import com.actian.spark_vector.vector.VectorJDBC
import com.actian.spark_vector.vector.VectorConnectionProperties
import com.actian.spark_vector.vector.TableSchemaGenerator
import com.actian.spark_vector.loader.options.{ UserOptions, VectorOptions }
import com.actian.spark_vector.loader.parsers.Args

import resource.managed


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
    val builder = SparkSession.builder.config(conf)
    val session = config.mode match {
      case Args.orcLoad.longName => builder.enableHiveSupport().getOrCreate()
      case _ => builder.getOrCreate()
    }

    val select = config.mode match {
      case Args.csvLoad.longName => CSVRead.registerTempTable(config, session.sqlContext)
      case Args.parquetLoad.longName => ParquetRead.registerTempTable(config, session.sqlContext)
      case Args.orcLoad.longName => OrcRead.registerTempTable(config, session.sqlContext)
      case Args.jsonLoad.longName => JSONRead.registerTempTable(config, session.sqlContext)
      case m => throw new IllegalArgumentException(s"Invalid configuration mode: ${m}")
    }
    
    val source = session.sql(select)
    val conn = VectorOptions.getConnectionProps(config.vector)
    if (config.general.createTable.getOrElse(false)) {
      /* Create a new Table in SQL context */
      val jdbc = new VectorJDBC(conn)
      jdbc.createTable(config.vector.targetTable, source.schema)
    }
    val mapping = getFieldMapping(source.schema, config.general.colsToLoad.getOrElse(Seq[String]()), conn, config.vector.targetTable)
    val df = checkSchemaDefaults(source, mapping, conn, config.vector.targetTable)
    df.rdd.loadVector(df.schema, conn, config.vector.targetTable, config.vector.preSQL, config.vector.postSQL, Option(mapping))
  }
  
  private def checkSchemaDefaults(source: DataFrame, fieldMapping: Map[String, String], conn: VectorConnectionProperties, table: String): DataFrame = {
    val jdbc = new VectorJDBC(conn)
    val defaults = collection.mutable.Map(jdbc.columnDefaults(table).toSeq: _*)
    jdbc.columnMetadata(table).foreach(c => if(c.nullable) defaults.remove(c.name))
    val sourceDefaults = defaults.map(f => (fieldMapping.find(_._2 == f._1).get._1 -> f._2))
    source.na.fill(sourceDefaults.toMap)
  }
  
  private def getFieldMapping(sourceSchema: StructType, colsToLoad: Seq[String], conn: VectorConnectionProperties, table: String): Map[String, String] = {
    val jdbc = new VectorJDBC(conn)
    val tableSchema = jdbc.columnMetadata(table)
    
    require(colsToLoad.size == tableSchema.size || sourceSchema.size == tableSchema.size, "Number of source columns to load does not match number of target columns in table")
    val fieldMapping = if (!colsToLoad.isEmpty) {
      require(colsToLoad.size == tableSchema.size, "Number of columns to load does not match number of target columns in table")
      (for (i <- 0 until colsToLoad.size) yield (colsToLoad(i) -> tableSchema(i).name)).toMap
    } else {
      require(sourceSchema.size == tableSchema.size, "Number of source columns do not match number of target columns in table")
      (for (i <- 0 until sourceSchema.size) yield (sourceSchema(i).name -> tableSchema(i).name)).toMap
    }
    
    fieldMapping
  }
}
