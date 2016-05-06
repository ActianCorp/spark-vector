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
  @inline private final def timeInNanos(source: Timestamp): Long =
    (source.getTime / PowersOfTen(MillisecondsScale)) * PowersOfTen(NanosecondsScale) + source.getNanos

  final def normalizeTime(source: Timestamp): Long = normalizeNanos(timeInNanos(source))

  private final def normalizeNanos(nanos: Long): Long = {
    val remainder = Math.abs(nanos % NanosecondsInDay)
    if (remainder >= 0) {
      remainder
    } else {
      remainder + NanosecondsInDay
    }
  }

  @inline final def scaleNanos(unscaledNanos: Long, scale: Int): Long = unscaledNanos / PowersOfTen(NanosecondsScale - scale)

  @inline final def unscaleNanos(scaledNanos: Long, scale: Int): Long = scaledNanos * PowersOfTen(NanosecondsScale - scale)

  @inline private final def convertLocalDateHelper(date: Date, sign: Int = 1): Unit = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    date.setTime(date.getTime + sign * cal.get(Calendar.ZONE_OFFSET) + sign * cal.get(Calendar.DST_OFFSET))
  }

  final def convertLocalDateToUTC(date: Date): Unit = convertLocalDateHelper(date)

  final def convertUTCToLocalDate(date: Date): Unit = convertLocalDateHelper(date, -1)

  @inline private final def convertLocalTimeHelper(time: Timestamp, sign: Int = 1): Unit = {
    val cal = Calendar.getInstance()
    val nanos = time.getNanos
    cal.setTimeInMillis(time.getTime)
    time.setTime(time.getTime + sign * cal.get(Calendar.ZONE_OFFSET) + sign * cal.get(Calendar.DST_OFFSET))
    time.setNanos(nanos)
  }

  final def convertLocalTimestampToUTC(time: Timestamp): Unit = convertLocalTimeHelper(time)

  final def convertUTCToLocalTimestamp(time: Timestamp): Unit = convertLocalTimeHelper(time, -1)

  /**
   * This trait should be used when implementing a type of time conversion,
   * for example a timezone converter using the upper helper functions.
   */
  trait TimeConverter {
    def convert(nanos: Long, scale: Int): Long
    def deconvert(source: Long, scale: Int): Long
  }
}
