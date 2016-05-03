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

/** Helper functions and constants for `Timestamp` conversions. */
object TimestampConversion {
  // scalastyle:off magic.number
  private final val SecondsBeforeEpochBI = BigInteger.valueOf(SecondsBeforeEpoch)
  private final val NanosecondsFactorBI = BigInteger.valueOf(PowersOfTen(NanosecondsScale).toLong)

  final def scaleTimestamp(epochSeconds: Long, subsecNanos: Long, scale: Int): BigInteger  = {
    val secondsTotal = BigInteger.valueOf(epochSeconds).add(SecondsBeforeEpochBI)
    val nanosTotal = secondsTotal.multiply(NanosecondsFactorBI).add(BigInteger.valueOf(subsecNanos))
    val adjustment = scale - NanosecondsScale
    val adjustmentFactor = BigInteger.valueOf(PowersOfTen(Math.abs(adjustment)))

    if (adjustment >= 0) {
      nanosTotal.multiply(adjustmentFactor)
    } else {
      nanosTotal.divide(adjustmentFactor)
    }
  }

  final def unscaleTimestamp(source: BigInteger, scale: Int): (Long, Long) = {
    val adjustment = scale - NanosecondsScale
    val adjustmentFactor = BigInteger.valueOf(PowersOfTen(Math.abs(adjustment)))

    val newSource = if (adjustment >= 0) {
      source.divide(adjustmentFactor)
    } else {
      source.multiply(adjustmentFactor)
    }

    val subsecNanosBI = newSource.mod(NanosecondsFactorBI)
    val epochSeconds = newSource.subtract(subsecNanosBI).divide(NanosecondsFactorBI).subtract(SecondsBeforeEpochBI)
    (epochSeconds.longValue, subsecNanosBI.longValue)
  }

  /**
   * This trait should be used when implementing a type of timestamp conversion,
   * for example a timestamp-zone converter using the upper helper functions.
   */
  trait TimestampConverter {
    def convert(epochSeconds: Long, subsecNanos: Long, scale: Int): BigInteger
    def deconvert(convertedValue: BigInteger, scale: Int): (Long, Long)
  }
  // scalastyle:on magic.number
}
