package com.actian.spark_vectorh.vector

import org.scalacheck.Gen
import org.scalacheck.Gen.oneOf

object VectorTypeGen {
  // Generate JDBC types returned by Vector
  val vectorJdbcTypeGen: Gen[String] =
    oneOf(
      "boolean",
      "integer1",
      "smallint",
      "integer",
      "bigint",
      "float4",
      "float",
      "decimal",
      "money",
      "char",
      "nchar",
      "varchar",
      "nvarchar",
      "ansidate",
      "time without time zone",
      "time with time zone",
      "time with local time zone",
      "timestamp without time zone",
      "timestamp with time zone",
      "timestamp with local time zone",
      "interval year to month",
      "interval day to year")
}
