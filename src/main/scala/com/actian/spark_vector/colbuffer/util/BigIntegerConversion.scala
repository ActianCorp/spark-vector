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

/** Helper functions and constants for `BigInteger` conversions. */
object BigIntegerConversion {
  // scalastyle:off magic.number
  final def convertToLongLongByteArray(value: BigInteger): Array[Byte] = {
    val source = value.toByteArray()
    val target = Array.fill[Byte](16)(0:Byte)
    val remaining = target.length - source.length
    var sourceIndex = source.length - 1
    var targetIndex = 0

    while (sourceIndex >= 0 && targetIndex < target.length) {
      target.update(targetIndex, source(sourceIndex))
      targetIndex += 1
      sourceIndex -= 1
    }

    if (remaining > 0) {
      var index = 0
      while (index < remaining) {
        target.update(source.length + index, if (value.signum() >= 0) 0 else 0xFF.toByte)
        index += 1
      }
    }

    target
  }
  // scalastyle:on magic.number
}
