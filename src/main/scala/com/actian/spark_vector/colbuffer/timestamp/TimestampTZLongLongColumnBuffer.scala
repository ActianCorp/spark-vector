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
package com.actian.spark_vector.colbuffer.timestamp

/** `ColumnBuffer` object for `time with time zone` types (scale [5, 9]). */
object TimestampTZLongLongColumnBuffer extends TimestampTZColumnBufferInstance with TimestampLongLongColumnBufferInstance {
  private final val MinTimestampTZLongLongScale = 5
  private final val MaxTimestampTZLongLongScale = 9

  override protected def adjustToUTC: Boolean = false

  private[colbuffer] override def supportsColumnType(tpe: String, precision: Int, scale: Int, nullable: Boolean): Boolean = {
    supportsTZColumnType(tpe, scale, MinTimestampTZLongLongScale, MaxTimestampTZLongLongScale)
  }
}
