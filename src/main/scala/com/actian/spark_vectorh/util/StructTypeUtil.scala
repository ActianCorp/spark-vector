package com.actian.spark_vectorh.util

import org.apache.spark.sql.types.{ DataType, StructField, StructType }

object StructTypeUtil {

  def createSchema(specs: (String, DataType)*): StructType =
    StructType(specs.map { case (n, t) => StructField(n, t, true) })

  implicit class RichStructType(val structType: StructType) extends AnyVal {
    def +(spec: (String, DataType)): StructType =
      spec match {
        case (n, t) => StructType(structType.fields :+ StructField(n, t, true))
      }
  }

  def fieldProperty(field: StructField, key: String): Option[String] =
    if (field.metadata.contains(key)) {
      Option(field.metadata.getString(key))
    } else {
      None
    }
}
