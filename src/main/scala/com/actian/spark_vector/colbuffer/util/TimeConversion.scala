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

import org.apache.spark.Logging

object TimeConversion extends Logging {
  final val MILLISECONDS_SCALE = 3
  final val MILLISECONDS_IN_MINUTE = 60 * 1000
  final val MILLISECONDS_IN_DAY = 24 * 60 * MILLISECONDS_IN_MINUTE
  final val NANOSECONDS_SCALE = 9;
  final val NANOSECONDS_IN_MILLI = 1000000;
  final val NANOSECONDS_IN_DAY = (MILLISECONDS_IN_DAY.toLong * NANOSECONDS_IN_MILLI)
  private final val POWERS_OF_TEN = Seq(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)

  final def timeInNanos(source: Timestamp): Long = {
    (source.getTime() / POWERS_OF_TEN(MILLISECONDS_SCALE)) * POWERS_OF_TEN(NANOSECONDS_SCALE) + source.getNanos()
  }

  final def normalizedTime(source: Timestamp): Long = {
    normalizedTime(timeInNanos(source))
  }

  final def normalizedTime(nanos: Long): Long = {
    val remainder = nanos % NANOSECONDS_IN_DAY
    if (remainder >= 0) {
      remainder
    } else {
      remainder + NANOSECONDS_IN_DAY
    }
  }

  final def scaledTime(source: Timestamp, scale: Int): Long = {
    scaledTime(timeInNanos(source), scale)
  }

  final def scaledTime(nanos: Long, scale: Int): Long = {
    val adjustment = NANOSECONDS_SCALE  - scale
    nanos / POWERS_OF_TEN(adjustment)
  }

  final def convertLocalDateToUTC(date: Date): Unit = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    date.setTime(date.getTime() + cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET))
  }

  final def convertLocalTimestampToUTC(time: Timestamp): Unit = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time.getTime())
    val nanos = time.getNanos()
    time.setTime(time.getTime() + cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET))
    time.setNanos(nanos)
  }

  trait TimeConverter {
    def convert(source: Timestamp, scale: Int): Long = {
      convert(timeInNanos(source), scale)
    }

    def convert(nanos: Long, scale: Int): Long
  }
}
