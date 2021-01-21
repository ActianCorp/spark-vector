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
 * The `Spark-Vector` loader is a utility that facilitates loading files of different formats (for now `CSV`,`Parquet`, and `Orc` only) into
 * `Vector`, through `Spark` and using the `Spark-Vector` connector.
 * 
 * For CSV parsing, the Spark-Vector loader uses the csv method of the [[https://spark.apache.org/docs/2.1.0/api/scala/index.html#org.apache.spark.sql.DataFrameReader@csv(paths:String*):org.apache.spark.sql.DataFrame org.apache.spark.sql.DataFrame]] class.
 * 
 * Example:
 * 
 * This scala code snippet to read a csv file (executed in spark-shell):
 * {{{
 * sqlContext.sql("""CREATE TEMPORARY VIEW large_table
 * USING com.actian.spark_vector.sql.DefaultSource
 * OPTIONS (
 *  host "vectorhost",
 *  instance "VI",
 *  database "dbName",
 *  table "large_table"
 * )""")
 * 
 * sqlContext.sql("""CREATE TEMPORARY VIEW csv_files
 * USING csv
 * OPTIONS (path "hdfs://namenode:8020/data/csv_file*", header "false", sep "|")
 * """)
 * 
 * val results = sqlContext.sql("""insert into table large_table select * from csv_files""")
 * }}}
 * 
 * is equivalent to
 * {{{
 * spark-submit --master spark://spark_master:7077 --class com.actian.spark_vector.loader.Main
 *  \$SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.1.jar load csv -sf "hdfs://namenode:8020/data/csv_file*"
 *  -sc "|" -vh vectorhost -vi VI -vd dbName -tt large_table
 * }}}
 * 
 * To read a parquet file this 
 * {{{
 * sqlContext.read.parquet("hdfs://namenode:8020/data/parquet_file.parquet").registerTempTable("parquet_file")
 * sqlContext.sql("""insert into table large_table select * from parquet_file""")
 * }}}
 * 
 * is equivalent to
 * {{{
 * spark-submit --master spark://spark_master:7077 --class com.actian.spark_vector.loader.Main
 *  \$SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.1.jar load parquet -sf "hdfs://namenode:8020/data/parquet_file.parquet"
 *  -vh vectorhost -vi VI -vd dbName -tt large_table
 * }}}
 * 
 * To read a orc file this
 * {{{
 * sqlContext.read.orc("hdfs://namenode:8020/data/orc_file.orc").registerTempTable("orc_file")
 * sqlContext.sql("""insert into table large_table select * from orc_file""")
 * }}}
 * 
 * is equivalent to
 * {{{
 * spark-submit --master spark://spark_master:7077 --class com.actian.spark_vector.loader.Main
 *  \$SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.1.jar load orc -sf "hdfs://namenode:8020/data/orc_file.orc"
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
