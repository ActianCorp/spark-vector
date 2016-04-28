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

import org.apache.spark.sql.types.StructField

/** Wrap column metadata returned by JDBC and provide functions to support converting into a StructField.
 */
case class ColumnMetadata(val name: String, val typeName: String, val nullable: Boolean, val precision: Int, val scale: Int) extends Serializable {

  /** Convert the given column metadata into a `StructField` representing the column
   *  @return a new `StructField` instance
   */
  val structField: StructField = StructField(name, dataType, nullable)

  // Convert from column type name to StructField data type
  private[this] def dataType: org.apache.spark.sql.types.DataType = {
    import org.apache.spark.sql.types._
    VectorDataType(typeName) match {
      case VectorDataType.BooleanType => BooleanType
      case VectorDataType.ByteType => ByteType
      case VectorDataType.ShortType => ShortType
      case VectorDataType.IntegerType => IntegerType
      case VectorDataType.BigIntType => LongType
      case VectorDataType.FloatType => FloatType
      case VectorDataType.DoubleType => DoubleType
      case VectorDataType.DecimalType => DecimalType(precision, scale)
      case VectorDataType.CharType | VectorDataType.NcharType | VectorDataType.VarcharType | VectorDataType.NvarcharType => StringType
      case VectorDataType.DateType => DateType
      case VectorDataType.TimeType | VectorDataType.TimeLTZType | VectorDataType.TimeTZType |
        VectorDataType.TimestampType | VectorDataType.TimestampLTZType | VectorDataType.TimestampTZType => TimestampType
      case VectorDataType.IntervalYearToMonthType | VectorDataType.IntervalDayToSecondType => StringType
      case VectorDataType.NotSupported => throw new VectorException(ErrorCodes.InvalidDataType, s"Vector type not supported: $typeName")
    }
  }

  override def toString: String = {
    "name: " + name + "; typeName: " + typeName + "; nullable: " + nullable + "; precision: " + precision + "; scale: " + scale
  }
}
