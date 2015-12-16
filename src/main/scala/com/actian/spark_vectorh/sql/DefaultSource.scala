package com.actian.spark_vectorh.sql

import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }
import org.apache.spark.sql.sources.{ BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider }
import org.apache.spark.sql.types.StructType

import com.actian.spark_vectorh.sql.DefaultSource.createTableRef
import com.actian.spark_vectorh.vector.VectorJDBC

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with Logging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val tableRef = createTableRef(parameters)
    VectorRelation(tableRef, None, sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val tableRef = createTableRef(parameters)
    VectorRelation(tableRef, Some(schema), sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val tableRef = createTableRef(parameters)
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

    VectorRelation(tableRef, None, sqlContext)
  }
}

object DefaultSource {

  def createTableRef(parameters: Map[String, String]): TableRef = {
    val host = parameters("host")
    val instance = parameters("instance")
    val database = parameters("database")
    val table = parameters("table")
    val user =
      if (parameters.contains("user")) Some(parameters("user"))
      else None
    val password =
      if (parameters.contains("password")) Some(parameters("password"))
      else None
    TableRef(host, instance, database, user, password, table)
  }
}
