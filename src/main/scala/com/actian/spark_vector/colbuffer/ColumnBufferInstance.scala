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
package com.actian.spark_vector.colbuffer

private[colbuffer] trait ColumnBufferInstance[T] {
  /** Get a new instance of ColumnBuffer[T] only if it supports the column type */
  def apply(name: String, index: Int, tpe: String, precision: Int, scale: Int, nullable: Boolean, maxRowCount: Int): ColumnBuffer[T] = {
    assert(supportsColumnType(tpe, precision, scale, nullable))
    getNewInstance(name, index, precision, scale, nullable, maxRowCount)
  }
  /** Get a new instance of ColumnBuffer[T] w/o checking for column type support */
  private[colbuffer] def getNewInstance(name: String, index: Int, precision: Int, scale: Int, nullable: Boolean, maxRowCount: Int): ColumnBuffer[T]
  /** Check before getting a new instance whether this ColumnBuffer[T] supports the column type */
  private[colbuffer] def supportsColumnType(tpe: String, precision: Int, scale: Int, nullable: Boolean): Boolean
}
