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

import com.actian.spark_vector.colbuffer.LongLongSize

import java.math.BigInteger
import java.nio.ByteBuffer

/** Helper functions and constants for `BigInteger` conversions. */
object BigIntegerConversion {
  val bigIntArray = Array.fill[Byte](LongLongSize)(0:Byte) /** Keep it in big-endian */

  // scalastyle:off magic.number
  /**
   * Puts a BigInteger to a ByteBuffer in little-endian order.
   * @note We need little-endian (cpu-wise) ordering due to Vector storing data
   * like this and not in big-endian (network-wise)
   */
  final def putLongLongByteArray(buffer: ByteBuffer, value: BigInteger): Unit = {
    val source = value.toByteArray() /** This is in big-endian */
    val remaining = LongLongSize - source.length
    var sourceIndex = source.length - 1

    while (sourceIndex >= 0) { /** Put in little-endian order */
      buffer.put(source(sourceIndex))
      sourceIndex -= 1
    }

    if (remaining > 0) {
      var index = 0
      while (index < remaining) {
        if (value.signum() >= 0) buffer.put(0.toByte) else buffer.put(0xFF.toByte)
        index += 1
      }
    }
  }

  /**
   * Gets a BigInteger from a ByteBuffer in big-endian order.
   */
  final def getLongLongByteArray(buffer: ByteBuffer): BigInteger = {
    var sourceIndex = bigIntArray.length - 1

    while (sourceIndex >= 0) { /** Get in little-endian order */
      bigIntArray(sourceIndex) = buffer.get()
      sourceIndex -= 1
    }

    new BigInteger(bigIntArray)
  }
  // scalastyle:on magic.number
}
