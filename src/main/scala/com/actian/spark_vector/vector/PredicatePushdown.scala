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

import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions.{ col, expr, lit }
import org.apache.spark.sql.types.DataType

import com.actian.spark_vector.sql.sparkQuote
import com.actian.spark_vector.vector.VectorDataType._

/**
 * Object that brings together a set of functions that are used for implementing "Predicate Pushdown":
 * A Vector query such as "SELECT * from external_table where col = 10" should not result in the provider
 * sending the whole table to Vector, but only those tuples that pass the "col = 10" predicate.
 */
object PredicatePushdown extends Logging {

  /** Says whether or not Predicate Pushdown is supported on columns of the given `vectorDataType`. */
  def typeIsSafeForPredPD(vectorDataType: EnumVal): Boolean = vectorDataType match {
    /* Due to current Vector limitations, ValueRanges can't be produced for time[stamp]s with time zone,
     * so we couldn't properly test this. As such, we're temporarily disabling this optimization, just to be safe. */
    case TimeTZType | TimestampTZType => false
    case NotSupported => false
    case _ => true
  }

  /** Says whether or not Predicate Pushdown is supported on columns of the given `vectorTypeName`. */
  def typeIsSafeForPredPD(vectorTypeName: String): Boolean = typeIsSafeForPredPD(VectorDataType(vectorTypeName))

  /** Converts the given `vectorLiteral`to the corresponding Spark literal, according to the specified `VectorDataType`. */
  def vectorLiteralToSparkLiteral(vectorLiteral: String, vectorDataType: EnumVal, sparkContext: SparkContext): String =
    vectorDataType match {
      case MoneyType => {
        val vectorMoneyLiteralPrefix = "$ "
        require(vectorLiteral.startsWith(vectorMoneyLiteralPrefix))
        vectorLiteral.stripPrefix(vectorMoneyLiteralPrefix)
      }
      case BooleanType => {
        require(vectorLiteral == "false" || vectorLiteral == "true")
        /* In Spark 1.5.x and earlier, lit("false").cast(BooleanType) gives TRUE
           * because it's only looking at whether the string is empty or not.
           * They fixed it in 1.6.0. (SPARK-10442) */
        val sparkVersion = sparkContext.version.split('.').map(_.toInt)
        if (sparkVersion(0) <= 1 && sparkVersion(1) < 6 && vectorLiteral == "false") ""
        else vectorLiteral
      }
      case TimeType | TimeTZType | TimeLTZType => "1970-01-01 " + vectorLiteral /* prepend default epoch */
      case _ => vectorLiteral
    }

  /**
   * Textual representation of single value ranges for a particular column, consisting of a list of Strings.
   * First element denotes the type of range, which can currently be of two types:
   * - Bounded ranges = intervals with inclusive/exclusive lower and upper bounds
   *     e.g. "()", "(]", "[)", "[]"
   *     For this type of intervals, the second and third strings in the list represent the lower and upper bounds.
   * - Unbounded ranges = intervals for which either the lower or the upper bound is (-)infinity
   *     e.g. ">", ">=", "<=", "<"
   *     For this types of intervals, the list is of size two and the second string represents the only bound
   *     @note: We also include here ("==", x) as a shorter notation for ("[]", x, x)
   */
  type ValueRange = Seq[String]

  /**
   * A `ValueRanges` object is a sequence of `ValueRange` elements and it is associated with a certain column.
   * As such, all string bounds of all the `ValueRange`s are expected to be literals of that column's `VectorDataType`.
   */
  type ValueRanges = Seq[ValueRange]

  /** Creates a `Column` that represents the condition ("predicate"), in Spark's terms, that is expressed by the given `range` */
  def filterForColumn(column: Column, litToConstCol: String => Column, range: ValueRange): Column = range match {
    case Seq(cmpType, arg) => {
      val value = litToConstCol(arg)
      value.isNull or (cmpType match {
        case "<" => column < value
        case "<=" => column <= value
        case "==" => column === value
        case ">=" => column >= value
        case ">" => column > value
      })
    }
    case Seq(intervalType, arg1, arg2) => {
      val lowerBound = litToConstCol(arg1)
      val upperBound = litToConstCol(arg2)
      lowerBound.isNull or upperBound.isNull or (intervalType match {
        case "()" => column > lowerBound and column < upperBound
        case "(]" => column > lowerBound and column <= upperBound
        case "[)" => column >= lowerBound and column < upperBound
        case "[]" => column >= lowerBound and column <= upperBound
      })
    }
  }

  /**
   * Creates `Some(Column)` that represents the condition ("predicate"), in Spark's terms,
   * corresponding to the given `columnMetadata`, if the latter has some `ValueRanges` specified
   * and is of a type for which Predicate Pushdown is safe - @see `typeIsSafeForPredPD`.
   * Otherwise returns `None`.
   */
  def filterForColumn(columnMetadata: ColumnMetadata, sparkContext: SparkContext): Option[Column] = columnMetadata match {
    case ColumnMetadata(columnName, typeName, _, _, _, Some(valueRanges)) => valueRanges match {
      case Nil => {
        /* An empty range list means that no tuples should be selected (e.g. col < 0 and col > 0). */
        Some(expr("false"))
      }
      case _ if !typeIsSafeForPredPD(typeName) => {
        logDebug(s"Predicate pushdown not yet supported for columns of type: ${typeName}")
        None
      }
      case _ => {
        val vectorDataType = VectorDataType(typeName)
        val sparkDataType = columnMetadata.dataType
        val sparkColumn = col(sparkQuote(columnName))

        def litToConstCol(vectorLiteral: String): Column = {
          val sparkLiteral = vectorLiteralToSparkLiteral(vectorLiteral, vectorDataType, sparkContext)
          lit(sparkLiteral).cast(sparkDataType)
        }

        /* The condition for this column is a disjunction of conditions corresponding to
         * the individual `ValueRange`s in the list. */
        Some(valueRanges.map(filterForColumn(sparkColumn, litToConstCol, _)).reduceLeft(_ or _))
      }
    }
    case _ => None /* There were no value ranges specified */
  }

  /** Filters the given `df` according to the specified `ValueRanges`. */
  def applyFilters(df: DataFrame, columns: Seq[ColumnMetadata], sparkContext: SparkContext): DataFrame = {
    val filters = for {
      column <- columns
      filter <- filterForColumn(column, sparkContext)
    } yield {
      logDebug(s"Applying filter for ${column.name}: ${filter}")
      filter
    }

    if (filters.nonEmpty) {
      /* Compute a final predicate as a conjunction of all per-column predicates
       * and then apply that to the given df as a single filter. */
      df.filter(filters.reduceLeft(_ and _))
    } else {
      logDebug(s"No filters applied.")
      df
    }
  }
}
