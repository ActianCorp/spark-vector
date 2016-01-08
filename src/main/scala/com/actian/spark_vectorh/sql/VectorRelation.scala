package com.actian.spark_vectorh.sql

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, sources }
import org.apache.spark.sql.sources.{ BaseRelation, Filter, InsertableRelation, PrunedFilteredScan }
import org.apache.spark.sql.types.StructType

import com.actian.spark_vectorh.vector.{ VectorJDBC, VectorOps }

private[sql] class VectorRelation(tableRef: TableRef, userSpecifiedSchema: Option[StructType], override val sqlContext: SQLContext)
    extends BaseRelation with InsertableRelation with PrunedFilteredScan with Logging {

  import com.actian.spark_vectorh.vector.VectorOps._

  override def schema: StructType =
    userSpecifiedSchema.getOrElse(VectorRelation.structType(tableRef))

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    log.info("vector: insert")

    if (overwrite) {
      VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
        cxn.executeStatement(s"delete from ${tableRef.table}")
      }
    }

    log.info(s"Trying to insert rdd: $data into vectorH table")
    val anySeqRDD = data.rdd.map(row => row.toSeq)
    // TODO could expose other options in Spark parameters
    val rowCount = anySeqRDD.loadVectorH(data.schema, tableRef.table, tableRef.toConnectionProps)
    log.info(s"loaded ${rowCount} records into table ${tableRef.table}")
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.info("vector: buildScan: columns: " + requiredColumns.mkString(", "))
    log.info("vector: buildScan: filters: " + filters.mkString(", "))

    val columns =
      if (requiredColumns.isEmpty) {
        "*"
      } else {
        requiredColumns.mkString(",")
      }

    val whereClause = VectorRelation.generateWhereClause(filters)
    val results = VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
      cxn.query(s"select $columns from ${tableRef.table} $whereClause");
    }

    val rows = results.map(Row.fromSeq(_))
    sqlContext.sparkContext.parallelize(rows)
  }
}

object VectorRelation {

  def apply(tableRef: TableRef, userSpecifiedSchema: Option[StructType], sqlContext: SQLContext): VectorRelation = {
    new VectorRelation(tableRef, userSpecifiedSchema, sqlContext)
  }

  def apply(tableRef: TableRef, sqlContext: SQLContext): VectorRelation = {
    new VectorRelation(tableRef, None, sqlContext)
  }

  /** Obtain the structType containing the schema for the table referred by tableRef */
  def structType(tableRef: TableRef): StructType = {
    VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
      val structFields = cxn.columnMetadata(tableRef.table).map(_.structField)
      StructType(structFields)
    }
  }

  /** Quote the column name so that it can be used in VectorSQL statements */
  def quote(name: String): String = "\"" + name + "\""

  /**
   * Converts a Filter structure into an equivalent prepared Vector SQL statement that can be
   *  used directly with Vector jdbc. The parameters are stored in the second element of the
   *  returned tuple
   */
  def convertFilter(filter: Filter): (String, Seq[Any]) = {
    filter match {
      case sources.EqualTo(attribute, value) => (s"${quote(attribute)} = ?", Seq(value))
      case sources.LessThan(attribute, value) => (s"${quote(attribute)} < ?", Seq(value))
      case sources.LessThanOrEqual(attribute, value) => (s"${quote(attribute)} <= ?", Seq(value))
      case sources.GreaterThan(attribute, value) => (s"${quote(attribute)} > ?", Seq(value))
      case sources.GreaterThanOrEqual(attribute, value) => (s"${quote(attribute)} >= ?", Seq(value))
      case sources.In(attribute, values) =>
        (quote(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), values.toSeq)
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
    }
  }

  /**
   * Given a sequence of filters, generate the where clause that can be used in the prepared statement. TODO(Andrei): verify if
   * this actually works since we convert a seq of (String, Seq[Any]) into a string using mkString
   */
  def generateWhereClause(filters: Array[Filter]): String = {
    filters.map(convertFilter).mkString(" and ")
  }
}
