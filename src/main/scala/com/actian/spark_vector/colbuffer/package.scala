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
package com.actian.spark_vector

/** Implements buffering (and value serialization) for `Vector` columns */
package object colbuffer {
  /** Common constants (for types and sizes) */
  final val ByteSize = 1
  final val ByteTypeId1 = "tinyint"
  final val ByteTypeId2 = "integer1"
  final val ShortSize = 2
  final val ShortTypeId1 = "smallint"
  final val ShortTypeId2 = "integer2"
  final val IntSize = 4
  final val IntTypeId1 = "integer"
  final val IntTypeId2 = "integer4"
  final val LongSize = 8
  final val LongTypeId1 = "bigint"
  final val LongTypeId2 = "integer8"
  final val FloatSize = 4
  final val FloatTypeId1 = "real"
  final val FloatTypeId2 = "float4"
  final val DoubleSize = 8
  final val DoubleTypeId1 = "float"
  final val DoubleTypeId2 = "float8";
  final val DoubleTypeId3 = "double precision"
  final val LongLongSize = 16
  final val DecimalTypeId = "decimal"
  final val BooleanSize = 1
  final val BooleanTypeId = "boolean"
  final val DateSize = 4
  final val DateTypeId = "ansidate"
  final val CharTypeId = "char"
  final val NcharTypeId = "nchar"
  final val VarcharTypeId = "varchar"
  final val NvarCharTypeId = "nvarchar"
  final val TimeLZTypeId = "time with local time zone"
  final val TimeNZTypeId1 = "time"
  final val TimeNZTypeId2 = "time without time zone"
  final val TimeTZTypeId = "time with time zone"
  final val TimestampLZTypeId = "timestamp with local time zone"
  final val TimestampNZTypeId1 = "timestamp"
  final val TimestampNZTypeId2 = "timestamp without time zone"
  final val TimestampTZTypeId = "timestamp with time zone"
}
