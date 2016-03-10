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
package com.actian.spark_vector.colbuffer.decimal

import com.actian.spark_vector.colbuffer._

import java.lang.Number
import java.math.BigDecimal
import java.nio.ByteBuffer

private[colbuffer] abstract class DecimalColumnBuffer(maxValueCount: Int, valueWidth: Int, name: String, precision: Int, scale: Int, nullable: Boolean) extends
  ColumnBuffer[Number](maxValueCount, valueWidth, valueWidth, name, nullable) {

  override def put(source: Number, buffer: ByteBuffer): Unit = putScaled(movePoint(new BigDecimal(source.toString()), precision, scale), buffer)

  protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit

  private def movePoint(value: BigDecimal, precision: Int, scale: Int): BigDecimal = {
    val sourceIntegerDigits = value.precision() - value.scale()
    val targetIntegerDigits = precision - scale
    val moveRightBy = if (sourceIntegerDigits > targetIntegerDigits) {
      scale - (sourceIntegerDigits - targetIntegerDigits)
    } else {
      scale
    }
    value.movePointRight(moveRightBy)
  }
}

private[colbuffer] trait DecimalColumnBufferInstance extends ColumnBufferInstance {
  protected val minPrecision: Int
  protected val maxPrecision: Int

  private[colbuffer] override def supportsColumnType(tpe: String, precision: Int, scale: Int, nullable: Boolean): Boolean =
    tpe.equalsIgnoreCase(DecimalTypeId) && 0 < precision && 0 <= scale && scale <= precision && minPrecision <= precision && precision <= maxPrecision
}
