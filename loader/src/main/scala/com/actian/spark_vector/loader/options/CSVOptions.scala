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
package com.actian.spark_vector.loader.options

case class CSVOptions(
  header: Option[Seq[String]] = None,
  headerRow: Option[Boolean] = None,
  inferSchema: Option[Boolean] = None,
  encoding: Option[String] = None,
  separatorChar: Option[Char] = None,
  quoteChar: Option[Char] = None,
  escapeChar: Option[Char] = None,
  commentChar: Option[Char] = None,
  ignoreLeading: Option[Boolean] = None,
  ignoreTrailing: Option[Boolean] = None,
  nullValue: Option[String] = None,
  nanValue: Option[String] = None,
  positiveInf: Option[String] = None,
  negativeInf: Option[String] = None,
  dateFormat: Option[String] = None,
  timestampFormat: Option[String] = None,
  parseMode: Option[String] = None)
