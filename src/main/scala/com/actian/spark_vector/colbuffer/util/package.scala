/*
 * Copyright 2016 Actian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

import java.sql.Timestamp

/** Util functions for various type conversions */
package object util {
  // scalastyle:off magic.number
  final val PowersOfTen = Seq(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)
  final val SecondsBeforeEpoch = 62167219200L
  final val TimeMaskSize = 11
  final val SecondsInMinute = 60
  final val MinutesInHour = 60
  final val HoursInDay = 24
  final val SecondsInDay = SecondsInMinute * MinutesInHour * HoursInDay
  final val MillisecondsScale = 3
  final val MillisecondsInMinute = SecondsInMinute * PowersOfTen(MillisecondsScale)
  final val MillisecondsInHour = MinutesInHour * MillisecondsInMinute
  final val MillisecondsInDay = HoursInDay * MillisecondsInHour
  final val NanosecondsScale = 9
  final val NanosecondsInMinute = (MillisecondsInMinute.toLong * PowersOfTen(NanosecondsScale - MillisecondsScale))
  final val NanosecondsInHour = MinutesInHour * NanosecondsInMinute
  final val NanosecondsInDay = HoursInDay * NanosecondsInHour
  // scalastyle:on magic.number

  def floorDiv(x: Long, y: Long): Long = {
    val ret = x / y
    if (ret >= 0 || ret * y == x) ret else ret - 1
  }
}
