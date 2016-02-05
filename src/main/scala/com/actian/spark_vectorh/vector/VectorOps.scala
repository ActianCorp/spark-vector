package com.actian.spark_vectorh.vector

import com.actian.spark_vectorh.vector.Vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.Logging

/** Add vector operations to RecordRDD. */
trait VectorOps {

  /**
   * Wrapper class to expose Vector operations on RDDs
   *
   * @param rdd input RDD
   */
  implicit class VectorRDDOps(rdd: RDD[Seq[Any]]) extends Logging {

    /**
     * Load a target VectorH table from this RDD.
     *
     * @param schema Input RDD schema
     * @param targetTable name of the table to load
     * @param vectorProps connection properties to the Vector instance
     * @param preSQL set of SQL statements to execute before loading begins
     * @param postSQL set of SQL statements to run after loading completes successfully. This SQL is executed
     *                only if the load works. The load is not rolled back if executing the postSQL fails.
     * @param fieldMap map of input field names to target columns (optional)
     * @param createTable if true, generates and executes a SQL create table statement based on the RDD schema
     * @return a <code>LoaderResult</code> instance which contains results of the load operation
     */
    def loadVectorH(
      schema: StructType,
      targetTable: String,
      vectorProps: VectorConnectionProperties,
      preSQL: Option[Seq[String]] = None,
      postSQL: Option[Seq[String]] = None,
      fieldMap: Option[Map[String, String]] = None,
      createTable: Boolean = false): Long = {
      LoadVector.loadVectorH(rdd, schema, targetTable, vectorProps,
        preSQL, postSQL, fieldMap, createTable)
    }
  }
}

object VectorOps extends VectorOps
