package com.actian.spark_vectorh.test.util

import org.apache.spark.sql.types.{ DataType, StructField, StructType }

object StructTypeUtil {

  def createSchema(specs: (String, DataType)*): StructType =
    StructType(specs.map { case (n, t) => StructField(n, t, true) })
}
