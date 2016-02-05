package com.actian.spark_vectorh.vector

import java.rmi.dgc.VMID
import java.sql.SQLException

import org.apache.spark.Logging
import org.apache.spark.sql.types.StructType

import com.actian.spark_vectorh.vector.ErrorCodes._
import com.actian.spark_vectorh.vector.VectorJDBC.withJDBC

/**
 * Common Vector operations
 */
private[vector] object Vector extends Logging {

  /**
   * Return the table schema as a `StructType` for the given table.
   *
   * @param vectorProps VectorH connection properties
   * @param targetTable name of the target table
   * @return schema of the table as a `StructType`
   * @throws VectorException if the target table does not exist or if there are connection failures
   */
  def getTableSchema(vectorProps: VectorConnectionProperties, targetTable: String, createTableSQL: Option[String] = None): Seq[ColumnMetadata] = {
    try {
      withJDBC(vectorProps) { dbCxn =>
        if (!dbCxn.tableExists(targetTable)) {
          if (createTableSQL.isDefined) {
            createTableSQL.foreach(dbCxn.executeStatement)
          } else {
            logError(s"$targetTable: target table does not exist")
            throw new VectorException(noSuchTable, targetTable + ": target table does not exist")
          }
        }

        logDebug(s"$targetTable: target table exists")
        dbCxn.columnMetadata(targetTable)
      }
    } catch {
      case e: SQLException => throw new VectorException(sqlException, s"${vectorProps.toJdbcUrl}: Error connecting to Vector instance", e)
    }
  }

  private def uniqueString: String = new VMID().toString.toUpperCase.replaceAll("[:\\-]", "X")

  case class Field2Column(fieldName: String, columnName: String)

  /**
   * Apply the given field mapping of input fields to target columns.
   *
   * @param fieldMap map of input field names to target column names
   * @param rddSchema schema of the input data
   * @param tableSchema schema of the target table
   * @return sequence of tuples of type (field name, column name) enclosed in `Field2Column` case classes
   * @throws VectorException if a target table does not exist or a map is not provided and the
   *  cardinality of the input and target table are not equal
   */
  def applyFieldMap(fieldMap: Map[String, String], rddSchema: StructType, tableSchema: StructType): Seq[Field2Column] = {
    if (fieldMap.isEmpty) {
      if (rddSchema.fields.length != tableSchema.fields.length) {
        throw VectorException(
          invalidNumberOfInputs,
          "Without a field map, the number of input fields and target columns are expected to match. Specify a field mapping to map input fields to target columns.")
      }

      // Zip field names and columns names and convert to Field2Columns collection
      rddSchema.fieldNames.zip(tableSchema.fieldNames).map { case (x: String, y: String) => Field2Column(x, y) }
    } else {
      if (fieldMap.size > tableSchema.fields.length) {
        throw VectorException(
          invalidNumberOfInputs,
          "More input fields are defined in the field mapping than exist in the target table")
      }

      // Validate that all entries in the field map reference existing source fields
      fieldMap.keys.foreach(fieldName => {
        if (!rddSchema.fieldNames.contains(fieldName)) {
          throw new VectorException(
            noSuchSourceField,
            s"$fieldName: source field in field mapping does not exist in the input. Available field names are: ${rddSchema.fieldNames.mkString(", ")}")
        }
      })

      val fieldColumnNames = rddSchema.fieldNames.foldLeft(Seq[Field2Column]())((columnNames, inputFieldName) => {
        fieldMap.get(inputFieldName) match {
          case Some(targetColumnName) =>
            if (tableSchema.fieldNames.contains(targetColumnName)) {
              columnNames :+ Field2Column(inputFieldName, targetColumnName)
            } else {
              throw VectorException(
                noSuchColumn,
                s"A column with name '$targetColumnName' does not exist in the target table. Available column names are: ${tableSchema.fieldNames.mkString(", ")}")
            }
          case None => columnNames
        }
      })

      if (fieldColumnNames.length == 0) {
        throw VectorException(
          noColumnsMapped,
          "The given field map does not map from any input fields to any target columns.")
      }

      fieldColumnNames
    }
  }

  /**
   * Validate that the list of columns for the given target table schema are OK to load.
   * Ensures that non-null columns are being loaded. Throws an exception otherwise.
   *
   * @param tableSchema schema of the target table
   * @param columnNames columns being loaded
   *
   * @throws VectorException thrown if a non-null column is not being loaded
   */
  def validateColumns(tableSchema: StructType, columnNames: Seq[String]): Unit = {
    val ex = tableSchema.fields.find(field => !field.nullable && !columnNames.contains(field.name)).map(field =>
      VectorException(missingNonNullColumn, s"Column ${field.name} is defined NOT NULL but is not being loaded."))
    if (ex.isDefined) throw ex.get
  }
}
