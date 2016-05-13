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
package com.actian.spark_vector.vector

import java.util.TimeZone

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, StructField, StructType, TimestampType}

import com.actian.spark_vector.test.util.DateHelper.{ansiDateFor, timestampFor}

object DateTimeDataFixture {
  def timeRDD(sparkContext: SparkContext): (RDD[Seq[Any]], StructType) = createTimeRDD(sparkContext, timeData)

  private[vector] val tz = TimeZone.getTimeZone("GMT-06:00")

  private[vector] val utc = TimeZone.getTimeZone("UTC")

  private[vector] val timeData = Seq(
    Seq[Any](
      timestampFor(1995, 1, 22, 18, 3, 29, 234, tz),
      timestampFor(1996, 2, 22, 18, 3, 29, 234),
      timestampFor(1997, 2, 22, 18, 3, 29, 234),
      timestampFor(1998, 1, 22, 18, 3, 29, 234, tz),
      timestampFor(1999, 2, 22, 18, 3, 29, 234),
      timestampFor(2000, 2, 22, 18, 3, 29, 234),
      timestampFor(2015, 11, 23, 18, 3, 29, 123, tz),
      timestampFor(2015, 11, 23, 18, 3, 29, 123),
      ansiDateFor(1995, 2, 22)),
    Seq[Any](
      timestampFor(2015, 3, 2, 17, 52, 12, 678, tz),
      timestampFor(2015, 4, 2, 17, 52, 12, 678),
      timestampFor(2015, 4, 2, 17, 52, 12, 678),
      timestampFor(2015, 3, 2, 17, 52, 12, 678, tz),
      timestampFor(2015, 4, 2, 17, 52, 12, 678),
      timestampFor(2015, 4, 2, 17, 52, 12, 678),
      timestampFor(2015, 11, 13, 17, 52, 12, 123, tz),
      ansiDateFor(2015, 4, 2)))

  private def createTimeRDD(sparkContext: SparkContext, data: Seq[Seq[Any]]): (RDD[Seq[Any]], StructType) = {
    val schema = StructType(Seq(
      StructField("tswtz", TimestampType),
      StructField("tsntz", TimestampType),
      StructField("tsltz", TimestampType),
      StructField("tswtz4", TimestampType),
      StructField("tsntz4", TimestampType),
      StructField("tsltz4", TimestampType),
      StructField("tmwtz", TimestampType),
      StructField("tmntz", TimestampType),
      StructField("tmltz", TimestampType),
      StructField("tmwtz3", TimestampType),
      StructField("tmntz3", TimestampType),
      StructField("tmltz3", TimestampType),
      StructField("date", DateType)))

    (sparkContext.parallelize(data, 2), schema)
  }

  def createTimeTable(connectionProps: VectorConnectionProperties)(tableName: String): Unit = {
    VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
        s"""|create table ${tableName} (
            |  tswtz timestamp with time zone,
            |  tsntz timestamp without time zone,
            |  tsltz timestamp with local time zone,
            |  tswtz4 timestamp(4) with time zone,
            |  tsntz4 timestamp(4) without time zone,
            |  tsltz4 timestamp(4) with local time zone,
            |  tmwtz time with time zone,
            |  tmntz time without time zone,
            |  tmltz time with local time zone,
            |  tmwtz3 time(3) with time zone,
            |  tmntz3 time(3) without time zone,
            |  tmltz3 time(3) with local time zone,
            |  dt date
            |)""".stripMargin)
    }
  }
}
