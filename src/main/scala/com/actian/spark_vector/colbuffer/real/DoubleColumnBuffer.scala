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
import com.actian.spark_vector.vector.VectorDataType
import com.actian.spark_vector.colbuffer.util.PowersOfTen

import java.nio.ByteBuffer

private class DoubleColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Double, Double](p.name, p.maxValueCount, DoubleSize, DoubleSize, p.nullable) {
  override def put(source: Double, buffer: ByteBuffer): Unit = buffer.putDouble(source)

  override def get(buffer: ByteBuffer): Double = buffer.getDouble()
}

private class MoneyColumnBuffer(p: ColumnBufferBuildParams) extends DoubleColumnBuffer(p) {
  /** @note Vector stores `money` values as `double`s w/o scale and considers it only for printing */
  private final val ScaleFactor = PowersOfTen(p.scale)

  override def put(source: Double, buffer: ByteBuffer): Unit = buffer.putDouble(source * ScaleFactor)

  override def get(buffer: ByteBuffer): Double = buffer.getDouble() / ScaleFactor
}

/** Builds a `ColumnBuffer` object for `float`, `float8`, `double precision`, `dbl`, `money` types. */
private[colbuffer] object DoubleColumnBuffer extends ColumnBufferBuilder {
  private val buildDouble: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] =
    ofDataType(VectorDataType.DoubleType) andThen { new DoubleColumnBuffer(_) }

  private val buildMoney: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] =
    ofDataType(VectorDataType.MoneyType) andThen { new MoneyColumnBuffer(_) }

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildDouble orElse buildMoney
}
