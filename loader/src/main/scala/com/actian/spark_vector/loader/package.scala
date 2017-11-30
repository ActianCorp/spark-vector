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

/**
 * `Spark-Vector` loader
 *
 * The `Spark-Vector` loader is a utility that facilitates loading files of different formats (for now `CSV` and `Parquet` only) into
 * `Vector`, through `Spark` and using the `Spark-Vector` connector.
 *
 * For CSV parsing, the Spark-Vector loader uses the [[https://github.com/databricks/spark-csv spark-csv]] library.
 *
 * Example:
 *
 * This scala code snippet (executed in spark-shell):
 * {{{
 * sqlContext.sql("""CREATE TEMPORARY TABLE large_table
 * USING com.actian.spark_vector.sql.DefaultSource
 * OPTIONS (
 *  host "vectorhost",
 *  instance "VI",
 *  database "dbName",
 *  table "large_table"
 * )""")
 *
 * sqlContext.sql("""CREATE TEMPORARY TABLE csv_files
 * USING com.databricks.spark.csv
 * OPTIONS (path "hdfs://namenode:8020/data/csv_file*", header "false", delimiter "|", parserLib "univocity")
 * """)
 *
 * val results = sqlContext.sql("""insert into table large_table select * from csv_files""")
 * }}}
 *
 * is equivalent to
 * {{{
 * spark-submit --master spark://spark_master:7077 --class com.actian.spark_vector.loader.Main
 *  \$SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.0.jar load csv -sf "hdfs://namenode:8020/data/csv_file*"
 *  -sc "|" -vh vectorhost -vi VI -vd dbName -tt large_table
 * }}}
 *
 * and
 * {{{
 * sqlContext.read.parquet("hdfs://namenode:8020/data/parquet_file.parquet").registerTempTable("parquet_file")
 * sqlContext.sql("""insert into table large_table select * from parquet_file""")
 * }}}
 *
 * is equivalent to
 * {{{
 * spark-submit --master spark://spark_master:7077 --class com.actian.spark_vector.loader.Main
 *  \$SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.0.jar load parquet -sf "hdfs://namenode:8020/data/parquet_file.parquet"
 *  -vh vectorhost -vi VI -vd dbName -tt large_table
 * }}}
 *
 * Of course, by using the `Spark-Vector` connector directly, one can load arbitrarily complex relations (not only files) into Vector and
 * files of any format that `Spark` is able to read.
 *
 * For a complete list of options available, see [[loader.parsers.Args Args]].
 * @see [[loader.parsers.Args Args]].
 */
package object loader {

}
