package com.actian.spark_vectorh.vector

import org.apache.spark.sql.types._

/**
 * Wrap column metadata returned by JDBC and provide functions to support converting into a StructField.
 */
case class ColumnMetadata(val name: String, val typeName: String, val nullable: Boolean, val precision: Int, val scale: Int) extends Serializable {

  /**
   * Convert the given column metadata into a `StructField` representing the column
   * @return a new `StructField` instance
   */
  val structField: StructField = StructField(name, dataType, nullable)

  // Convert from column type name to StructField data type
  private[this] def dataType: DataType = {
    typeName match {
      case "boolean" => BooleanType
      case "integer1" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "bigint" => LongType
      case "float4" => FloatType
      case "float" => DoubleType
      case "decimal" => DecimalType(precision, scale)
      case "money" => DecimalType(precision, scale)
      case "char" => StringType
      case "nchar" => StringType
      case "varchar" => StringType
      case "nvarchar" => StringType
      case "ansidate" => DateType
      case "time without time zone" => TimestampType
      case "time with time zone" => TimestampType
      case "time with local time zone" => TimestampType
      case "timestamp without time zone" => TimestampType
      case "timestamp with time zone" => TimestampType
      case "timestamp with local time zone" => TimestampType
      case "interval year to month" => StringType
      case "interval day to year" => StringType
      case _ => StringType
    }
  }

  override def toString: String = {
    "name: " + name + "; typeName: " + typeName + "; nullable: " + nullable + "; precision: " + precision + "; scale: " + scale
  }
}
