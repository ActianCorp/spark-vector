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

import java.nio.ByteBuffer
import java.sql.Date
import java.time.LocalDate

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.vector.VectorDataType
import org.apache.spark.internal.Logging

private class DateColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Date, Int](p.name, p.maxValueCount, DateSize, DateSize, p.nullable) with Logging {
  private final val DaysBeforeUnixEpoch = 719528

  override def put(source: Date, buffer: ByteBuffer): Unit = {
	val prolepticGregDate = source.toLocalDate()
	val days_x100 = prolepticGregDate.toEpochDay() + DaysBeforeUnixEpoch
	if(!days_x100.isValidInt)
	{
		logError(s"Date ${prolepticGregDate.toString()} is out of currently supported range!")
		throw new IllegalArgumentException(s"Date ${prolepticGregDate.toString()} is out of currently supported range!")
	}
	else{
		buffer.putInt(days_x100.toInt)
	}
  }

  override def get(buffer: ByteBuffer): Int = {
    val days = buffer.getInt()
	days - DaysBeforeUnixEpoch
  }
}

/** Builds a `ColumnBuffer` object for `ansidate` types. */
private[colbuffer] object DateColumnBuffer extends ColumnBufferBuilder {
  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] =
    ofDataType(VectorDataType.DateType) andThen { new DateColumnBuffer(_) }
}
