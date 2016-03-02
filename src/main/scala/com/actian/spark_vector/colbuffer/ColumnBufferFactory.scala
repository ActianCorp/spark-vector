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
package com.actian.spark_vector.colbuffer

import org.apache.spark.Logging

import com.actian.spark_vector.colbuffer.integer._
import com.actian.spark_vector.colbuffer.real._
import com.actian.spark_vector.colbuffer.decimal._
import com.actian.spark_vector.colbuffer.singles._
import com.actian.spark_vector.colbuffer.string._
import com.actian.spark_vector.colbuffer.time._
import com.actian.spark_vector.colbuffer.timestamp._

object ColumnBufferFactory extends Logging {
  private final val columnBufs:List[ColumnBufferInstance[_]] = List(
    ByteColumnBuffer,
    ShortColumnBuffer,
    IntColumnBuffer,
    LongColumnBuffer,
    FloatColumnBuffer,
    DoubleColumnBuffer,
    DecimalByteColumnBuffer,
    DecimalShortColumnBuffer,
    DecimalIntColumnBuffer,
    DecimalLongColumnBuffer,
    DecimalLongLongColumnBuffer,
    BooleanColumnBuffer,
    DateColumnBuffer,
    ConstantLengthSingleByteStringColumnBuffer,
    ConstantLengthSingleCharStringColumnBuffer,
    ConstantLengthMultiByteStringColumnBuffer,
    ConstantLengthMultiCharStringColumnBuffer,
    VariableLengthByteStringColumnBuffer,
    VariableLengthCharStringColumnBuffer,
    TimeLZIntColumnBuffer,
    TimeLZLongColumnBuffer,
    TimeNZIntColumnBuffer,
    TimeNZLongColumnBuffer,
    TimeTZIntColumnBuffer,
    TimeTZLongColumnBuffer,
    TimestampLZLongColumnBuffer,
    TimestampLZLongLongColumnBuffer,
    TimestampNZLongColumnBuffer,
    TimestampNZLongLongColumnBuffer,
    TimestampTZLongColumnBuffer,
    TimestampTZLongLongColumnBuffer
  )

  def apply(name: String, index: Int, tpe: String, precision: Int, scale: Int, nullable: Boolean, maxRowCount: Int): Option[ColumnBuffer[_]] = {
    columnBufs.find(columnBuf => columnBuf.supportsColumnType(tpe, precision, scale, nullable))
              .map(columnBuf => columnBuf.getNewInstance(name, index, precision, scale, nullable, maxRowCount))
  }
}
