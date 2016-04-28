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
case class ColumnMetadata(val name: String, val typeName: String, val nullable: Boolean, val precision: Int, val scale: Int) extends Serializable {
  /**
   * Convert the given column metadata into a `StructField` representing the column
   * @return a new `StructField` instance
   */
  val structField: StructField = StructField(name, dataType, nullable)

  /**
   * Convert from column type name to data type and retain also its maximum allocation size
   */
  private[this] def dataTypeInfo = typeName match {
    case ByteTypeId1 | ByteTypeId2 => (ByteType, ByteSize)
    case ShortTypeId1 | ShortTypeId2 => (ShortType, ShortSize)
    case IntTypeId1 | IntTypeId2 => (IntegerType, IntSize)
    case LongTypeId1 | LongTypeId2 => (LongType, LongSize)
    case FloatTypeId1 | FloatTypeId2 => (FloatType, FloatSize)
    case DoubleTypeId1 | DoubleTypeId2 | DoubleTypeId3 => (DoubleType, DoubleSize)
    case DecimalTypeId1 | DecimalTypeId2 => (DecimalType(precision, scale), LongLongSize)
    case BooleanTypeId => (BooleanType, ByteSize)
    case DateTypeId => (DateType, IntSize)
    case CharTypeId | NcharTypeId => (StringType, IntSize)
    case VarcharTypeId | NvarcharTypeId | YearToMonthTypeId | DayToSecondTypeId => (StringType, precision + 1)
    case TimeNZTypeId1 | TimeNZTypeId2 | TimeTZTypeId | TimeLZTypeId => (TimestampType, LongSize)
    case TimestampNZTypeId1 | TimestampNZTypeId2  | TimestampTZTypeId  | TimestampLZTypeId  => (TimestampType, LongLongSize)
    case _ => throw new Exception(s"Unsupported type '${typeName}' for column '${name}'.")
  }

  def dataType: DataType = dataTypeInfo._1
  def maxDataSize: Int = dataTypeInfo._2

  override def toString: String =
    s"name: ${name}, typeName: ${typeName}, dataType: ${dataType}, nullable: ${nullable}, precision: ${precision}, scale: ${scale}"
}
