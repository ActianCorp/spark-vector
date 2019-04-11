 /*
 * Copyright 2019 Actian Corporation
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

case class JSONOptions(
  header: Option[Seq[String]] = None,
  primitivesAsString: Option[Boolean] = None,
  allowComments: Option[Boolean] = None,
  allowUnquoted: Option[Boolean] = None,
  allowSingleQuotes: Option[Boolean] = None,
  allowLeadingZeros: Option[Boolean] = None,
  allowEscapingAny: Option[Boolean] = None,
  allowUnquotedControlChars: Option[Boolean] = None,
  multiline: Option[Boolean] = None,
  parseMode: Option[String] = None)
  
