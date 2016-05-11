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
package com.actian.spark_vector.colbuffer.string

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util.StringConversion
import com.actian.spark_vector.vector.VectorDataType

import org.apache.spark.unsafe.types.UTF8String

import java.nio.ByteBuffer

private[colbuffer] abstract class ByteEncodedStringColumnBuffer(p: ColumnBufferBuildParams)
    extends ColumnBuffer[String, UTF8String](p.name, p.maxValueCount, p.precision + 1, ByteSize, p.nullable) {
  override def put(source: String, buffer: ByteBuffer): Unit = {
    buffer.put(encode(source))
    buffer.put(0.toByte)
  }

  protected def encode(value: String): Array[Byte]

  override def get(buffer: ByteBuffer): UTF8String = {
    /** @note Do not reuse the byteArr. UTF8String doesn't make a copy of it internally. */
    val byteArr = Array.fill(p.precision + 1)(0.toByte)
    var i = 0
    do {
      byteArr(i) = buffer.get()
      i += 1
    } while (byteArr(i - 1) != 0.toByte && i < byteArr.length)
    UTF8String.fromBytes(byteArr, 0, i - 1)
  }
}

private class ByteLengthLimitedStringColumnBuffer(p: ColumnBufferBuildParams) extends ByteEncodedStringColumnBuffer(p) {
  override protected def encode(str: String): Array[Byte] = StringConversion.truncateToUTF8Bytes(str, p.precision)
}

private class CharLengthLimitedStringColumnBuffer(p: ColumnBufferBuildParams) extends ByteEncodedStringColumnBuffer(p.copy(
  precision = p.precision * CharLengthLimitedStringColumnBuffer.MaxUTF8CharSize)) {
  override protected def encode(str: String): Array[Byte] = StringConversion.truncateToUTF16CodeUnits(str, p.precision)
}

private object CharLengthLimitedStringColumnBuffer {
  private final val MaxUTF8CharSize = IntSize
}

/** Builds a `ColumnBuffer` object for `char`, `nchar`, `varchar`, `nvarchar` byte-encoded types. */
private[colbuffer] object ByteEncodedStringColumnBuffer extends ColumnBufferBuilder {
  private val buildConstLenMultiPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if p.precision > 1 => p
  }

  private val buildConstLenMulti: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildConstLenMultiPartial andThenPartial {
    (ofDataType(VectorDataType.CharType) andThen { new ByteLengthLimitedStringColumnBuffer(_) }) orElse
      (ofDataType(VectorDataType.NcharType) andThen { new CharLengthLimitedStringColumnBuffer(_) })
  }

  private val buildVarLenPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if p.precision > 0 => p
  }

  private val buildVarLen: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildVarLenPartial andThenPartial {
    (ofDataType(VectorDataType.VarcharType) andThen { new ByteLengthLimitedStringColumnBuffer(_) }) orElse
      (ofDataType(VectorDataType.NvarcharType) andThen { new CharLengthLimitedStringColumnBuffer(_) })
  }

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildConstLenMulti orElse buildVarLen
}
