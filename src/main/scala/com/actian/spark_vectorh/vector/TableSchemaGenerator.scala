package com.actian.spark_vectorh.vector

import org.apache.spark.sql.types._

object TableSchemaGenerator {

  // TODO ensure consistency with ColumnMetadata mappings
  private def sqlTypeName(dataType: DataType): String = dataType match {
    case BooleanType => "boolean"
    case ByteType => "integer1"
    case ShortType => "smallint"
    case IntegerType => "integer"
    case LongType => "bigint"
    case FloatType => "float4"
    case DoubleType => "float"
    case dec: DecimalType => s"decimal(${dec.precision}, ${dec.scale})"
    case DateType => "ansidate"
    case TimestampType => "timestamp with time zone"
    case _ => "varchar(4096)"
  }

  private def columnSpec(field: StructField): String =
    s"${field.name} ${sqlTypeName(field.dataType)}${if (field.nullable) "" else " NOT NULL"}"

  /**
   * Given SparkSQL datatypes specified by `schema`, generate the VectorSQL create table
   *  statement that matches the datatypes
   */
  def generateTableSQL(name: String, schema: StructType): String = {
    val columnSpecs = schema.fields.map(columnSpec)
    s"""|CREATE TABLE ${name} (
        |${columnSpecs.mkString("\t", ",\n\t", "")}
        |)
     """.stripMargin
  }
}
