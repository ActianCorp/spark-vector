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

private[colbuffer] abstract class IntegerEncodedStringColumnBuffer(p: ColumnBufferBuildParams) extends
  ColumnBuffer[String](p.name, p.maxValueCount, IntSize, IntSize, p.nullable) {
  override def put(source: String, buffer: ByteBuffer): Unit = if (source.isEmpty()) {
    buffer.putInt(IntegerEncodedStringColumnBuffer.Whitespace)
  } else {
    buffer.putInt(encode(source))
  }

  protected def encode(str: String): Int

  override def get(buffer: ByteBuffer): String = ???
}

private class ConstantLengthSingleByteStringColumnBuffer(p: ColumnBufferBuildParams) extends IntegerEncodedStringColumnBuffer(p) {
  override protected def encode(value: String): Int = if (StringConversion.truncateToUTF8Bytes(value, 1).length == 0) {
    IntegerEncodedStringColumnBuffer.Whitespace
  } else {
    value.codePointAt(0)
  }
}

private class ConstantLengthSingleCharStringColumnBuffer(p: ColumnBufferBuildParams) extends IntegerEncodedStringColumnBuffer(p) {
  override protected def encode(value: String): Int = if (Character.isHighSurrogate(value.charAt(0))) {
    IntegerEncodedStringColumnBuffer.Whitespace
  } else {
    value.codePointAt(0)
  }
}

/** Builds a `ColumnBuffer` object for `char`, `nchar` integer-encoded types. */
private[colbuffer] object IntegerEncodedStringColumnBuffer extends ColumnBufferBuilder {
  final val Whitespace = '\u0020'

  private val buildPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if p.precision == 1 => p
  }

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = buildPartial andThenPartial {
    /** `ColumnBuffer` object for `char` types (with precision == 1). */
    case p if p.tpe == CharTypeId => new ConstantLengthSingleByteStringColumnBuffer(p)
    /** `ColumnBuffer` object for `nchar` types (with precision == 1). */
    case p if p.tpe == NcharTypeId => new ConstantLengthSingleCharStringColumnBuffer(p)
  }
}
