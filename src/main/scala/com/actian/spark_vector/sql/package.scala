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

/** Implements `SparkSQL` API, including a `BaseRelation` and a `RelationProvider` */
package object sql {
  def sparkQuote(name: String): String = s"`$name`"

  def colsSelectStatement(cols: Option[Seq[String]]): String = cols match {
    case None | Some(Nil) => "*" /* it is legal in SparkSql to execute 'select * from zero_cols_table' */
    case Some(seq) => seq.map(sparkQuote).mkString(", ")
  }
}
