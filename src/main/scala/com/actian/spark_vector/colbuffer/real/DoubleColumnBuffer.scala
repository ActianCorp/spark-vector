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

private class DoubleColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Double](p.name, p.maxValueCount, DoubleSize, DoubleSize, p.nullable) {
  override def put(source: Double, buffer: ByteBuffer): Unit = buffer.putDouble(source)

  override def get(buffer: ByteBuffer): Double = buffer.getDouble()
}

/** Builds a `ColumnBuffer` object for `float`, `float8`, `double precision` types. */
private[colbuffer] object DoubleColumnBuffer extends ColumnBufferBuilder {
  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = {
    case p if p.tpe == DoubleTypeId1 || p.tpe == DoubleTypeId2 || p.tpe == DoubleTypeId3 => new DoubleColumnBuffer(p)
  }
}
