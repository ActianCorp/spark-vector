
# Spark VectorH Connector

A library to integrate VectorH with Spark, allowing you to load Spark DataFrames/RDDs into VectorH in parallel and to consume results of VectorH based computations in Spark(SQL).

## Requirements

This library requires:
* Spark 1.5.1.
* [scala-arm](https://github.com/jsuereth/scala-arm)

## Building (from source)

Spark-VectorH connector is built with [sbt](http://www.scala-sbt.org/). To build, run:

    sbt package

## Using with Spark shell/submit
This module can be added to Spark using the `--jars` command line option, together with the Actian Vector JDBC connector (`lib/iijdbc.jar`) and [scala-arm](https://github.com/jsuereth/scala-arm).
Spark shell example (assuming `$SPARK_VECTORH` is the root directory of spark-vectorh):
    
    $SPARK_HOME/bin/spark-shell --jars $SPARK_VECTORH/target/target/spark_vectorh-1.0-SNAPSHOT.jar,$SPARK_VECTORH/lib/iijdbc.jar,$HOME/.ivy2/cache/com.jsuereth/scala-arm_2.10/jars/scala-arm_2.10-1.3.jar

## Usage

### SparkSQL

```
sqlContext.sql("""CREATE TEMPORARY TABLE vectorh_table
USING com.actian.spark_vectorh.sql.DefaultSource
OPTIONS (
    host "hostname",
    instance "VH",
    database "databasename",
    table "my_table"
)""")
```

and then to load data into VectorH:
    
    sqlContext.sql("insert into vectorh_table select * from spark_table")

... or to read VectorH data in:
    
    sqlContext.sql("select * from vectorh_table")

### Spark-VectorH Loader

The Spark-Vectorh loader is a command line client utility that provides the ability to load CSV and Parquet files through Spark into VectorH, using the Spark-VectorH connector.

#### Building

Since the loader depends on the connector, first publish locally the connector artifacts:
    
    cd $SPARK_VECTORH && sbt publish-local

and then

    cd $SPARK_VECTORH_LOADER && sbt assembly

#### Usage

Assuming that there is a VectorH Installation on leader node `leader`, instance `C1` and database `testDB`

#### CSV

Loading CSV files:
    
```
spark-submit --class com.actian.spark_vectorh.cli.Main $SPARK_VECTORH_LOADER/target/scala-2.10/spark_vectorh_client-assembly-1.0-SNAPSHOT.jar load csv -sf hdfs://namenode:8020/tmp/file.csv
-vh leader -vi C1 -vd testDB -tt my_table -sc " "
```

#### Parquet

Loading Parquet files:
    
```
spark-submit --class com.actian.spark_vectorh.cli.Main $SPARK_VECTORH_LOADER/target/scala-2.10/spark_vectorh_client-assembly-1.0-SNAPSHOT.jar load parquet -sf hdfs://namenode:8020/tmp/file.parquet
-vh leader -vi C1 -vd testDB -tt my_table -sc
```