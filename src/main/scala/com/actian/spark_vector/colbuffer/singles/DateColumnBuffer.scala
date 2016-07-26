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
package com.actian.spark_vector.colbuffer.singles

import java.nio.ByteBuffer
import java.sql.Date
import java.util.{ Calendar, TimeZone }

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util._
import com.actian.spark_vector.vector.VectorDataType

private class DateColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Date, Int](p.name, p.maxValueCount, DateSize, DateSize, p.nullable) {
  private final val DaysBeforeEpoch = 719528
  private val cal = Calendar.getInstance
  private val calIsNotUTC = cal.getTimeZone != TimeZone.getTimeZone("UTC")

  override def put(source: Date, buffer: ByteBuffer): Unit = {
    if (calIsNotUTC) {
      TimeConversion.convertLocalDateToUTC(source, cal)
    }
    buffer.putInt((source.getTime() / MillisecondsInDay + DaysBeforeEpoch).toInt)
  }

  override def get(buffer: ByteBuffer): Int = buffer.getInt() - DaysBeforeEpoch
}

/** Builds a `ColumnBuffer` object for `ansidate` types. */
private[colbuffer] object DateColumnBuffer extends ColumnBufferBuilder {
  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] =
    ofDataType(VectorDataType.DateType) andThen { new DateColumnBuffer(_) }
}
