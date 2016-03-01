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

import java.math.BigInteger

object TimestampConversion {
  final val SECONDS_IN_MINUTE = 60
  final val SECONDS_BEFORE_EPOCH = 62167219200L
  final val SECONDS_BEFORE_EPOCH_BI = BigInteger.valueOf(SECONDS_BEFORE_EPOCH)
  final val NANOSECONDS_SCALE = 9
  final val NANOSECONDS_FACTOR_BI = BigInteger.valueOf(Math.pow(10, NANOSECONDS_SCALE).toLong)

  def scaledTimestamp(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger  = {
    val secondsTotal = BigInteger.valueOf(epochSeconds).add(BigInteger.valueOf(offsetSeconds)).add(SECONDS_BEFORE_EPOCH_BI)
    val nanosTotal = secondsTotal.multiply(NANOSECONDS_FACTOR_BI).add(BigInteger.valueOf(subsecNanos))
    val adjustment = scale - NANOSECONDS_SCALE
    val adjustmentFactor = BigInteger.valueOf(Math.pow(10, Math.abs(adjustment)).toLong)

    if (adjustment >= 0) {
      nanosTotal.multiply(adjustmentFactor)
    } else {
      nanosTotal.divide(adjustmentFactor)
    }
  }

  trait TimestampConverter {
    def convert(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger
  }
}
