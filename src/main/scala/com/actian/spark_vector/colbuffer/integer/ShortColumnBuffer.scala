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
package com.actian.spark_vector.colbuffer.integer

import com.actian.spark_vector.colbuffer._

import java.nio.ByteBuffer

private class ShortColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Short](p.name, p.maxValueCount, ShortSize, ShortSize, p.nullable) {
  override def put(source: Short, buffer: ByteBuffer): Unit = buffer.putShort(source)

  override def get(buffer: ByteBuffer): Short = buffer.getShort()
}

/** Builds a `ColumnBuffer` object for `smallint`, `integer2` types. */
private[colbuffer] object ShortColumnBuffer extends ColumnBufferBuilder {
  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = {
    case p if p.tpe == ShortTypeId1 || p.tpe == ShortTypeId2 => new ShortColumnBuffer(p)
  }
}
