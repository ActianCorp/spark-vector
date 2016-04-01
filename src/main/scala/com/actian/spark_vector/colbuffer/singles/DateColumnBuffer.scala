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
package com.actian.spark_vector.colbuffer.singles

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util._

import java.nio.ByteBuffer
import java.sql.Date

private class DateColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Date](p.name, p.maxValueCount, DateSize, DateSize, p.nullable) {
  override protected def put(source: Date, buffer: ByteBuffer): Unit = {
    TimeConversion.convertLocalDateToUTC(source)
    buffer.putInt((source.getTime() / MillisecondsInDay + DateColumnBuffer.DaysBeforeEpoch).toInt)
  }

  override protected def putOne(source: ByteBuffer) = ???

  override def get() = ???
}

/** Builds a `ColumnBuffer` object for `ansidate` types. */
private[colbuffer] object DateColumnBuffer extends ColumnBufferBuilder {
  private final val DaysBeforeEpoch = 719528

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = {
    case p if p.tpe == DateTypeId => new DateColumnBuffer(p)
  }
}
