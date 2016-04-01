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

import java.nio.ByteBuffer

private[colbuffer] abstract class ByteEncodedStringColumnBuffer(p: ColumnBufferBuildParams) extends
  ColumnBuffer[String](p.name, p.maxValueCount, p.precision + 1, ByteSize, p.nullable) {
  override protected def put(source: String, buffer: ByteBuffer): Unit = {
    buffer.put(encode(source))
    buffer.put(0:Byte)
  }

  protected def encode(str: String): Array[Byte]

  override protected def putOne(source: ByteBuffer) = ???

  override def get() = ???
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

  private val buildConstLenMulti: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = buildConstLenMultiPartial andThenPartial {
    /** `ColumnBuffer` object for `char` types (with precision > 1). */
    case p if p.tpe == CharTypeId => new ByteLengthLimitedStringColumnBuffer(p)
    /** `ColumnBuffer` object for `nchar` types (with precision > 1). */
    case p if p.tpe == NcharTypeId => new CharLengthLimitedStringColumnBuffer(p)
  }

  private val buildVarLenPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if p.precision > 0 => p
  }

  private val buildVarLen: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = buildVarLenPartial andThenPartial {
    /** `ColumnBuffer` object for `varchar` types (with precision > 0). */
    case p if p.tpe == VarcharTypeId => new ByteLengthLimitedStringColumnBuffer(p)
    /** `ColumnBuffer` object for `nvarchar` types (with precision > 0). */
    case p if p.tpe == NvarcharTypeId => new CharLengthLimitedStringColumnBuffer(p)
  }

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = buildConstLenMulti orElse buildVarLen
}
