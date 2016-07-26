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
package com.actian.spark_vector.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object RDDUtil {
  /**
   * Select the given subset of field names (`fields`) from `rdd` and return a subset of the `schema` with a new `RDD` with the values reordered by the `fields` name list.
   */
  def selectFields(rdd: RDD[Row], schema: StructType, fields: Seq[String]): (RDD[Row], StructType) = {
    if (fields.length == 0) throw new IllegalArgumentException("The list of fields to select must contain at least one field name")
    val fieldNameSet = fields.toSet
    if (fields.length != fieldNameSet.size) throw new IllegalArgumentException("The given fields names must be unique, fields cannot be repeated")
    val subsetType = schema.apply(fieldNameSet)

    if (subsetType == schema) {
      (rdd, schema)
    } else {
      val fieldIndices = fields.map(fieldName => schema.fieldNames.indexOf(fieldName))
      /** Copy from current row structure into wanted row structure */
      (rdd.map(row => Row.fromSeq(fieldIndices.map(row(_)))), subsetType)
    }
  }

  /**
   * Map the input columns (as specified in `inputType`) to the target columns (as specified by `targetType`)
   *
   * @return Either None if the `targetType` has exactly the same columns as `inputType`
   *  or An array of size equal to `targetType`.fields.size containing a mapping of `targetType` fields to `inputType` fields
   */
  def targetToInput(inputType: StructType, targetType: StructType, targetToInput: Map[String, String]): Option[IndexedSeq[Option[Int]]] = if (inputType.fields.size > targetType.fields.size) {
    throw new IllegalArgumentException(s"There are more fields in the input type: $inputType than in the target type: $targetType")
  } else if (inputType == targetType) {
    None
  } else {
    val inputNamesToIdx = (0 until inputType.fields.size).map { idx => inputType.fieldNames(idx) -> idx }.toMap
    Some((0 until targetType.fields.size).map { idx => targetToInput.get(targetType.fieldNames(idx)).map(inputNamesToIdx.get(_)).flatten })
  }
}
