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
  // scalastyle:off magic.number
  /**
   * Puts a BigInteger to a ByteBuffer using little-endian ordering.
   * @note We need little-endian ordering due to network serialization
   * otherwise, the BigInteger.toByteArray would've been sufficient.
   */
  final def putLongLongByteArray(buffer: ByteBuffer, value: BigInteger): Unit = {
    val source = value.toByteArray() /** This is in big-endian */
    val remaining = LongLongSize - source.length
    var sourceIndex = source.length - 1

    while (sourceIndex >= 0) {
      buffer.put(source(sourceIndex))
      sourceIndex -= 1
    }

    if (remaining > 0) {
      var index = 0
      while (index < remaining) {
        if (value.signum() >= 0) buffer.put(0:Byte) else buffer.put(0xFF.toByte)
        index += 1
      }
    }
  }
  // scalastyle:on magic.number
}
