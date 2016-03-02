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

object StringConversion {
  private final val UTF8CHARSET = Charset.forName("UTF-8")
  private final val HIGH_BIT_MASK = 0x80.toByte
  private final val MULTI_BYTE_START_MASK = 0xC0.toByte
  private final val EMPTY_STRING = Array[Byte]()

  def truncateToUTF8Bytes(value: String, targetSize: Int): Array[Byte] = {
    val bytes = value.getBytes(UTF8CHARSET)

    if (bytes.length <= targetSize) {
      /** Encoded bytes fit within wanted size */
      bytes
    } else {
      /**
       *  Find from the end of the array the first byte which is a single
       *  byte character or the start of a multi-byte character
       */
      def condHigh(i: Int): Boolean = (bytes(i) & HIGH_BIT_MASK) != HIGH_BIT_MASK
      def condMulti(i: Int): Boolean = (bytes(i) & MULTI_BYTE_START_MASK) == MULTI_BYTE_START_MASK
      val ret = (targetSize to 0 by -1).toList
                                       .find(i => condHigh(i) || condMulti(i))
                                       .map(i => if (condHigh(i)) bytes.slice(0, Math.min(targetSize, i + 1)) else bytes.slice(0, i))
      if (ret.isDefined) {
        ret.get
      } else {
        /** No characters from the source fit in the target size buffer */
        EMPTY_STRING
      }
    }
  }

  def truncateToUTF16CodeUnits(value: String, targetSize: Int): Array[Byte] = {
    if (value.length() <= targetSize) {
      value.getBytes(UTF8CHARSET);
    } else {
      if (Character.isHighSurrogate(value.charAt(targetSize - 1))) {
        value.substring(0, targetSize - 1).getBytes(UTF8CHARSET);
      } else {
        value.substring(0, targetSize).getBytes(UTF8CHARSET);
      }
    }
  }
}
