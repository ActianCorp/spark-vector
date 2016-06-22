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

import com.actian.spark_vector.colbuffer._

import org.apache.spark.sql.types._

/**
 * Wrap column metadata returned by JDBC and provide functions to support converting into a StructField.
 */
case class ColumnMetadata(val name: String,
    val typeName: String,
    val nullable: Boolean,
    val precision: Int,
    val scale: Int) extends Serializable {
  /**
   * Convert the given column metadata into a `StructField` representing the column
   *
   * @return a new `StructField` instance
   */
  lazy val structField: StructField = StructField(name, dataType, nullable)

  /** Convert from column type name to data type and retain also its maximum allocation size */
  private[this] def dataTypeInfo = VectorDataType(typeName) match {
    case VectorDataType.ByteType => (ByteType, ByteSize)
    case VectorDataType.ShortType => (ShortType, ShortSize)
    case VectorDataType.IntegerType => (IntegerType, IntSize)
    case VectorDataType.BigIntType => (LongType, LongSize)
    case VectorDataType.FloatType => (FloatType, FloatSize)
    case VectorDataType.DoubleType | VectorDataType.MoneyType => (DoubleType, DoubleSize)
    case VectorDataType.BooleanType => (BooleanType, ByteSize)
    case VectorDataType.DecimalType => (DecimalType(precision, scale), LongLongSize)
    case VectorDataType.DateType => (DateType, IntSize)
    case VectorDataType.CharType | VectorDataType.NcharType => (StringType, if (precision == 1) IntSize else precision + 1)
    case VectorDataType.VarcharType | VectorDataType.NvarcharType |
      VectorDataType.IntervalYearToMonthType | VectorDataType.IntervalDayToSecondType => (StringType, precision + 1)
    case VectorDataType.TimeType | VectorDataType.TimeLTZType | VectorDataType.TimeTZType => (TimestampType, LongSize)
    case VectorDataType.TimestampType | VectorDataType.TimestampLTZType | VectorDataType.TimestampTZType => (TimestampType, LongLongSize)
    case VectorDataType.NotSupported =>
      throw new VectorException(ErrorCodes.InvalidDataType, s"Unsupported vector type '${typeName}' for column '${name}'.")
  }

  def dataType: DataType = dataTypeInfo._1
  def maxDataSize: Int = dataTypeInfo._2

  override def toString: String =
    s"name: ${name}, typeName: ${typeName}, dataType: ${dataType}, nullable: ${nullable}, precision: ${precision}, scale: ${scale}"
}
