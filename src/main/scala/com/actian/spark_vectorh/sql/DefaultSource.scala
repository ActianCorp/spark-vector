package com.actian.spark_vectorh.sql

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

import com.actian.spark_vectorh.vector.VectorJDBC

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with Logging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    VectorRelation(TableRef(parameters), sqlContext)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation =
    VectorRelation(TableRef(parameters), Some(schema), sqlContext)

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val tableRef = TableRef(parameters)
    val table = VectorRelation(tableRef, sqlContext)

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
