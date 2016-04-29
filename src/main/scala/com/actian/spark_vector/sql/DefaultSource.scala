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

import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }
import org.apache.spark.sql.sources.{ BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider }
import org.apache.spark.sql.types.StructType

import com.actian.spark_vector.vector.VectorJDBC

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with Logging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    VectorRelation(TableRef(parameters), sqlContext, parameters)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation =
    VectorRelation(TableRef(parameters), Some(schema), sqlContext, parameters)

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val tableRef = TableRef(parameters)
    val table = VectorRelation(tableRef, sqlContext, parameters)

    mode match {
      case SaveMode.Overwrite =>
        table.insert(data, true)
      case SaveMode.ErrorIfExists =>
        val isEmpty = VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
          cxn.isTableEmpty(tableRef.table)
        }
        if (isEmpty) {
          table.insert(data, false)
        } else {
          throw new UnsupportedOperationException("Writing to a non-empty Vector table is not allowed with mode ErrorIfExists.")
        }
      case SaveMode.Append =>
        table.insert(data, false)
      case SaveMode.Ignore =>
        val isEmpty = VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
          cxn.isTableEmpty(tableRef.table)
        }
        if (isEmpty) {
          table.insert(data, false)
        }
    }

    table
  }
}
