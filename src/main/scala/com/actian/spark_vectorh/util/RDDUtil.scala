package com.actian.spark_vectorh.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

object RDDUtil {
  /**
   *  Select the given subset of field names (`fields`) from `rdd` and return a subset of the `schema` with a new `RDD` with the values reordered by the `fields` name list.
   */
  def selectFields(rdd: RDD[Seq[Any]], schema: StructType, fields: Seq[String]): (RDD[Seq[Any]], StructType) = {
    if (fields.length == 0) throw new IllegalArgumentException("The list of fields to select must contain at least one field name")
    val fieldNameSet = fields.toSet
    if (fields.length != fieldNameSet.size) throw new IllegalArgumentException("The given fields names must be unique, fields cannot be repeated")
    val subsetType = schema.apply(fields.toSet)

    if (subsetType == schema) {
      (rdd, schema)
    } else {
      val fieldIndices = fields.map(fieldName => schema.fieldNames.indexOf(fieldName))
      // Copy from current row structure into wanted row structure
      (rdd.map(row => fieldIndices.map(row(_))), subsetType)
    }
  }

  /**
   * Convert the given `rdd` with schema specified by `inputType` to a new `RDD` with schema `targetType`, mapping columns from input
   *  to target using the `targetToInput` map and filling the missing columns with nulls
   */
  def fillWithNulls(rdd: RDD[Seq[Any]], inputType: StructType, targetType: StructType, targetToInput: Map[String, String]): RDD[Seq[Any]] = {
    if (inputType.fields.size > targetType.fields.size) {
      throw new IllegalArgumentException("There are more fields in the input type than in the target type")
    }
    val inputNamesToIdx = (0 until inputType.fields.size).map { idx => inputType.fieldNames(idx) -> idx }.toMap
    val targetNamesToIdx = (0 until targetType.fields.size).map {
      case idx =>
        targetToInput.get(targetType.fieldNames(idx)).map(inputNamesToIdx.get(_)).flatten
    }

    rdd.map { case row => (0 until targetNamesToIdx.size).map(targetNamesToIdx(_).map(row).getOrElse(null)) }
  }
}
