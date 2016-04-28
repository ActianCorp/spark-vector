package com.actian.spark_vector.vector

object VectorDataType {
  sealed trait EnumVal
  case object BooleanType extends EnumVal
  case object ByteType extends EnumVal
  case object ShortType extends EnumVal
  case object IntegerType extends EnumVal
  case object BigIntType extends EnumVal
  case object FloatType extends EnumVal
  case object DoubleType extends EnumVal
  case object DecimalType extends EnumVal
  case object CharType extends EnumVal
  case object NcharType extends EnumVal
  case object VarcharType extends EnumVal
  case object NvarcharType extends EnumVal
  case object DateType extends EnumVal
  case object TimeType extends EnumVal
  case object TimeLTZType extends EnumVal
  case object TimeTZType extends EnumVal
  case object TimestampType extends EnumVal
  case object TimestampLTZType extends EnumVal
  case object TimestampTZType extends EnumVal
  case object IntervalYearToMonthType extends EnumVal
  case object IntervalDayToSecondType extends EnumVal
  case object NotSupported extends EnumVal

  def apply(name: String) = name match {
    case "boolean" | "bool" => BooleanType
    case "tinyint" | "integer1" | "schr" | "uchr" => ByteType
    case "smallint" | "integer2" | "ssht" | "usht" => ShortType
    case "integer" | "integer4" | "int" | "sint" | "uint" => IntegerType
    case "bigint" | "integer8" | "slng" | "ulng" | "uidx" => BigIntType
    case "float4" | "real" | "flt" => FloatType
    case "float" | "float8" | "double precision" | "dbl" => DoubleType
    case "decimal" | "money" => DecimalType
    case "char" => CharType
    case "nchar" => NcharType
    case "varchar" => VarcharType
    case "nvarchar" => NvarcharType
    case "ansidate" => DateType
    case "time without time zone" | "time" => TimeType
    case "time with time zone" | "timetz" => TimeTZType
    case "time with local time zone" | "timeltz" => TimeLTZType
    case "timestamp without time zone" | "timestamp" => TimestampType
    case "timestamp with time zone" | "timestamptz" => TimestampTZType
    case "timestamp with local time zone" | "timestampltz" => TimestampLTZType
    case "interval year to month" | "intervalym" => IntervalYearToMonthType
    case "interval day to second" | "intervalds" => IntervalDayToSecondType
    case _ => NotSupported
  }
}
