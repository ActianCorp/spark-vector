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
package com.actian.spark_vector.vector

import org.apache.spark.sql.{ types => sqltype }

object VectorDataType {
  sealed trait VectorTypeEnum {
    val vectorSQLtype: String
  }
  
  case object BooleanType extends VectorTypeEnum { val vectorSQLtype = "boolean" }
  case object ByteType extends VectorTypeEnum { val vectorSQLtype = "integer1" }
  case object ShortType extends VectorTypeEnum { val vectorSQLtype = "smallint" }
  case object IntegerType extends VectorTypeEnum { val vectorSQLtype = "integer" }
  case object BigIntType extends VectorTypeEnum { val vectorSQLtype = "bigint" }
  case object FloatType extends VectorTypeEnum { val vectorSQLtype = "float4" }
  case object DoubleType extends VectorTypeEnum { val vectorSQLtype = "float" }
  case object MoneyType extends VectorTypeEnum { val vectorSQLtype = "money" }
  case object DecimalType extends VectorTypeEnum { val vectorSQLtype = "decimal" }
  case object CharType extends VectorTypeEnum { val vectorSQLtype = "char" }
  case object NcharType extends VectorTypeEnum { val vectorSQLtype = "nchar" }
  case object VarcharType extends VectorTypeEnum { val vectorSQLtype = "varchar" }
  case object NvarcharType extends VectorTypeEnum { val vectorSQLtype = "nvarchar" }
  case object DateType extends VectorTypeEnum { val vectorSQLtype = "ansidate" }
  case object TimeType extends VectorTypeEnum { val vectorSQLtype = "time without time zone" }
  case object TimeLTZType extends VectorTypeEnum { val vectorSQLtype = "time with local time zone" }
  case object TimeTZType extends VectorTypeEnum { val vectorSQLtype = "time with time zone" }
  case object TimestampType extends VectorTypeEnum { val vectorSQLtype = "timestamp without time zone" }
  case object TimestampLTZType extends VectorTypeEnum { val vectorSQLtype = "timestamp with local time zone" }
  case object TimestampTZType extends VectorTypeEnum { val vectorSQLtype = "timestamp with time zone" }
  case object IntervalYearToMonthType extends VectorTypeEnum { val vectorSQLtype = "interval year to month" }
  case object IntervalDayToSecondType extends VectorTypeEnum { val vectorSQLtype = "interval day to second" }
  case object IPV4Type extends VectorTypeEnum { val vectorSQLtype = "ipv4" }
  case object IPV6Type extends VectorTypeEnum { val vectorSQLtype = "ipv6" }
  case object UUIDType extends VectorTypeEnum { val vectorSQLtype = "uuid" }
  case object NotSupported extends VectorTypeEnum { val vectorSQLtype = "" }
  
  def vectorDataTypes: List[VectorTypeEnum] = List(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      BigIntType,
      FloatType,
      DoubleType,
      MoneyType,
      DecimalType,
      CharType,
      NcharType,
      VarcharType,
      NvarcharType,
      DateType,
      TimeType,
      TimeLTZType,
      TimeTZType,
      TimestampType,
      TimestampLTZType,
      TimestampTZType,
      IntervalYearToMonthType,
      IntervalDayToSecondType,
      IPV4Type,
      IPV6Type,
      UUIDType
      )

  def apply(name: String): VectorTypeEnum = name match {
    case "tinyint" | "integer1" | "schr" | "uchr" => ByteType
    case "smallint" | "integer2" | "ssht" | "usht" => ShortType
    case "integer" | "integer4" | "int" | "sint" | "uint" => IntegerType
    case "bigint" | "integer8" | "slng" | "ulng" | "uidx" => BigIntType
    case "float4" | "real" | "flt" => FloatType
    case "float" | "float8" | "double precision" | "dbl" => DoubleType
    case "money" => MoneyType
    case "boolean" | "bool" => BooleanType
    case "decimal" => DecimalType
    case "char" => CharType
    case "nchar" => NcharType
    case "varchar" => VarcharType
    case "nvarchar" => NvarcharType
    case "ansidate" | "date" => DateType
    case "time without time zone" | "time" => TimeType
    case "time with time zone" | "timetz" => TimeTZType
    case "time with local time zone" | "timeltz" => TimeLTZType
    case "timestamp without time zone" | "timestamp" => TimestampType
    case "timestamp with time zone" | "timestamptz" => TimestampTZType
    case "timestamp with local time zone" | "timestampltz" => TimestampLTZType
    case "interval year to month" | "intervalym" => IntervalYearToMonthType
    case "interval day to second" | "intervalds" => IntervalDayToSecondType
    case "ipv4" => IPV4Type
    case "ipv6" => IPV6Type
    case "uuid" | "byte" => UUIDType
    case _ => NotSupported
  }
  
  def apply(sqlType: sqltype.DataType): VectorTypeEnum = sqlType match {
    case sqltype.BooleanType => BooleanType
    case sqltype.ByteType => ByteType
    case sqltype.ShortType => ShortType
    case sqltype.IntegerType => IntegerType
    case sqltype.LongType => BigIntType
    case sqltype.FloatType => FloatType
    case sqltype.DoubleType => DoubleType
    case sqltype.DecimalType() => DecimalType
    case sqltype.DateType => DateType
    case sqltype.TimestampType => TimestampTZType
    case _ => VarcharType
  }
  
}
