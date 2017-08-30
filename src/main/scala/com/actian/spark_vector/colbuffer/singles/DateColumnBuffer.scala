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
  private final val JulianBoundary = 578101
  private final val CenturyDays = 36524

  override def put(source: Date, buffer: ByteBuffer): Unit = {
    val cal = Calendar.getInstance
    cal.set(source.getYear, source.getMonth, source.getDate())
    val dayOfYear = cal.get(Calendar.DAY_OF_YEAR)
    val year = source.getYear + 1900
    // Need to convert to proleptic gregorian calendar date
    var days = (year * 365) + ((year - 1) / 4) - (year / 100) + (year / 400) + dayOfYear
    // Need to adjust for error in Jan-Feb of certain century years
    if (year % 100 == 0 && year % 400 != 0 && dayOfYear < 61) days += 1
    buffer.putInt(days)
  }

  override def get(buffer: ByteBuffer): Int = {
    val days = buffer.getInt()
    var offset = 0
    // Need to convert from proleptic gregorian to julian if date before 1582/10/14
    if (days < JulianBoundary) {
      val n = (days - 366) / CenturyDays
      offset = n  - (n / 4 + 2)
      // Need to adjust for error in Jan-Feb of certain century years
      val cdays = days % CenturyDays
      val qdays = days % (CenturyDays * 4)
      if (qdays > 365 && cdays < 366 && cdays > (59 + n / 4)) {
        offset += 1
      }
    }
    days - DaysBeforeEpoch + offset
  }
}

/** Builds a `ColumnBuffer` object for `ansidate` types. */
private[colbuffer] object DateColumnBuffer extends ColumnBufferBuilder {
  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] =
    ofDataType(VectorDataType.DateType) andThen { new DateColumnBuffer(_) }
}
