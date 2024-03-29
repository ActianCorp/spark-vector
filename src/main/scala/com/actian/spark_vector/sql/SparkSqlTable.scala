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
package com.actian.spark_vector.sql

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.sql.DataFrame

sealed trait SparkSqlTable {
  def tableName: String
  def quotedName: String = sparkQuote(tableName)
  def close(): Unit
}

class SkeletonTable extends SparkSqlTable{ 
  override def tableName: String = "" 
  override def close(): Unit = {}
}

case class HiveTable(override val tableName: String) extends SparkSqlTable {
  override def close(): Unit = {}
}

class TempTable private (override val tableName: String, df: DataFrame) extends SparkSqlTable {
  private def register(): Unit = df.createOrReplaceTempView(tableName)
  override def close(): Unit = df.sqlContext.dropTempTable(tableName);
}

object TempTable {
  private val id = new AtomicLong(0L)

  def apply(tableNameBase: String, df: DataFrame): TempTable = {
    val tableName = s"${tableNameBase}_${id.incrementAndGet}"
    val tt = new TempTable(tableName, df)
    tt.register()
    tt
  }
}
