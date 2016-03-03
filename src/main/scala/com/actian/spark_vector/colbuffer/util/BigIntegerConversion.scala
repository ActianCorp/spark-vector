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
    var resultIndex = 0

    for (sourceIndex <- source.length - 1 to 0 by -1 if resultIndex < target.length) {
      target.update(resultIndex, source(sourceIndex))
      resultIndex += 1
    }

    if (remaining > 0) {
      for (index <- 0 to remaining - 1) {
        target.update(source.length + index, if (value.signum() >= 0) 0 else 0xFF.toByte)
      }
    }

    target
  }
  // scalastyle:on magic.number
}
