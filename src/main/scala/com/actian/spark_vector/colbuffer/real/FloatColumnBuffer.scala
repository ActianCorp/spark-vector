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
package com.actian.spark_vector.colbuffer.real

import com.actian.spark_vector.colbuffer._

import java.nio.ByteBuffer

private class FloatColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Float, Float](p.name, p.maxValueCount, FloatSize, FloatSize, p.nullable) {
  override def put(source: Float, buffer: ByteBuffer): Unit = buffer.putFloat(source)

  override def get(buffer: ByteBuffer): Float = buffer.getFloat()
}

/** Builds a `ColumnBuffer` object for `real`, `float4` types. */
private[colbuffer] object FloatColumnBuffer extends ColumnBufferBuilder {
  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = {
    case p if p.tpe == FloatTypeId1 || p.tpe == FloatTypeId2 => new FloatColumnBuffer(p)
  }
}
