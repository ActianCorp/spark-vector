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

import java.nio.charset.Charset;

/** Helper functions and constants for `String` conversions. */
object StringConversion {
  private final val UTF8Charset = Charset.forName("UTF-8")
  private final val HighBitMask = 0x80.toByte
  private final val MultiByteStartMask = 0xC0.toByte
  private final val EmptyString = Array[Byte]()

  def truncateToUTF8Bytes(value: String, targetSize: Int): Array[Byte] = {
    val bytes = value.getBytes(UTF8Charset)

    if (bytes.length <= targetSize) {
      /** Encoded bytes fit within wanted size */
      bytes
    } else {
      /**
       * Find from the end of the array the first byte which is a single
       * byte character or the start of a multi-byte character
       */
      var i = targetSize
      var ret = EmptyString
      def condHigh(i: Int): Boolean = (bytes(i) & HighBitMask) != HighBitMask
      def condMulti(i: Int): Boolean = (bytes(i) & MultiByteStartMask) == MultiByteStartMask

      while (i >= 0) {
        if (condHigh(i)) {
          ret = bytes.slice(0, Math.min(targetSize, i + 1))
        } else if (condMulti(i)) {
          ret = bytes.slice(0, i)
        }
        i = if (ret == EmptyString) i - 1 else -1
      }

      ret
    }
  }

  def truncateToUTF16CodeUnits(value: String, targetSize: Int): Array[Byte] = if (value.length() <= targetSize) {
    value.getBytes(UTF8Charset)
  } else if (Character.isHighSurrogate(value.charAt(targetSize - 1))) {
    value.substring(0, targetSize - 1).getBytes(UTF8Charset);
  } else {
    value.substring(0, targetSize).getBytes(UTF8Charset);
  }
}
