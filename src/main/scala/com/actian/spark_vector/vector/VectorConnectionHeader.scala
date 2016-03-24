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
package com.actian.spark_vector.vector

/**
 * Container for Vector connection partial header message.
 * @note the rest of the header's information (e.g. num columns, column data types, nullability, etc.)
 * is not stored within this container since we got/used it directly from a JDBC query.
 */
case class VectorConnectionHeader(statusCode: Int, vectorSize: Int)

object VectorConnectionHeader {
  final val StatusCodeIndex = 0
  final val VectorSizeIndex = 8
}
