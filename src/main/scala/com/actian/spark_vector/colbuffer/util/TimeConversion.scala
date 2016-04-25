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
package com.actian.spark_vector.colbuffer.util

import java.sql.Timestamp
import java.sql.Date
import java.util.Calendar

/** Helper functions and constants for `Time` conversions. */
object TimeConversion {
  final def timeInNanos(source: Timestamp): Long =
    (source.getTime / PowersOfTen(MillisecondsScale)) * PowersOfTen(NanosecondsScale) + source.getNanos

  final def normalizedTime(source: Timestamp): Long = normalizedTime(timeInNanos(source))

  final def normalizedTime(nanos: Long): Long = {
    val remainder = Math.abs(nanos % NanosecondsInDay)
    if (remainder >= 0) {
      remainder
    } else {
      remainder + NanosecondsInDay
    }
  }

  final def scaledTime(unscaledNanos: Long, scale: Int): Long = unscaledNanos / PowersOfTen(NanosecondsScale  - scale)

  final def unscaledTime(scaledNanos: Long, scale: Int): Long = scaledNanos * PowersOfTen(NanosecondsScale - scale)

  final def convertLocalDateToUTC(date: Date): Unit = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    date.setTime(date.getTime + cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET))
  }

  final def convertLocalTimestampToUTC(time: Timestamp): Unit = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time.getTime)
    val nanos = time.getNanos
    time.setTime(time.getTime + cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET))
    time.setNanos(nanos)
  }

  /** This trait should be used when implementing a type of time conversion,
   *  for example a time-zone converter using the upper helper functions. */
  trait TimeConverter {
    def convert(nanos: Long, scale: Int): Long
    def deconvert(source: Long, scale: Int): Long
  }
}
