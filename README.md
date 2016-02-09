
# Spark Vector Connector

A library to integrate Vector with Spark, allowing you to load Spark DataFrames/RDDs into Vector in parallel and to consume results of Vector based computations in Spark(SQL).
This connector works with both Vector SMP and VectorH MPP.

## Requirements

This library requires:
* Spark 1.5.1.
* [scala-arm](https://github.com/jsuereth/scala-arm)

## Building (from source)

Spark-Vector connector is built with [sbt](http://www.scala-sbt.org/). To build, run:

    sbt assembly

## Using with Spark shell/submit
This module can be added to Spark using the `--jars` command line option. Spark shell example (assuming `$SPARK_VECTOR` is the root directory of spark-vector):

    spark-shell --jars $SPARK_VECTOR/target/spark_vector-assembly-1.0-SNAPSHOT.jar

## Usage

### SparkSQL

```
sqlContext.sql("""CREATE TEMPORARY TABLE vector_table
USING com.actian.spark_vector.sql.DefaultSource
OPTIONS (
    host "hostname",
    instance "VI",
    database "databasename",
    table "my_table"
)""")
```

and then to load data into Vector:

    sqlContext.sql("insert into vector_table select * from spark_table")

... or to read Vector data in:

    sqlContext.sql("select * from vector_table")

#### Options
The `OPTIONS` clause of the SparkSQL statement can contain:
<table cellpadding="3" cellspacing="3">
 <tr>
    <th>Parameter</th>
    <th>Required</th>
    <th>Default</th>
    <th>Notes</th>
 </tr>
 <tr>
    <td><tt>host</tt></td>
    <td>Yes</td>
    <td>none</td>
    <td>Host name of where Vector is located</td>
 </tr>
 <tr>
    <td><tt>instance</tt></td>
    <td>Yes</td>
    <td>none</td>
    <td>Vector database instance identifier (two letters)</td>
 </tr>
 <tr>
    <td><tt>database</tt></td>
    <td>Yes</td>
    <td>none</td>
    <td>Vector database name</td>
 </tr>
 <tr>
    <td><tt>user</tt></td>
    <td>No</td>
    <td>empty string</td>
   <td>User name to use when connecting to Vector</td>
 </tr>
 <tr>
    <td><tt>password</tt></td>
    <td>No</td>
    <td>empty string</td>
    <td>Password to use when connecting to Vector</td>
 </tr>
 <tr>
    <td><tt>table</tt></td>
    <td>Yes</td>
    <td>None</td>
    <td>Vector target table</td>
 </tr>
</table>

### Spark-Vector Loader

The Spark-Vector loader is a command line client utility that provides the ability to load CSV and Parquet files through Spark into Vector, using the Spark-Vector connector.

#### Building

    sbt loader/assembly

#### Usage

Assuming that there is a Vector Installation on node `vectorhost`, instance `VI` and database `testDB`

#### CSV

Loading CSV files:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-1.0-SNAPSHOT.jar load csv -sf hdfs://namenode:8020/tmp/file.csv
-vh vectorhost -vi VI -vd testDB -tt my_table -sc " "
```

#### Parquet

Loading Parquet files:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-1.0-SNAPSHOT.jar load parquet -sf hdfs://namenode:8020/tmp/file.parquet
-vh vectorhost -vi VI -vd testDB -tt my_table
```

The entire list of options for CSV loading can be retrieved with:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-1.0-SNAPSHOT.jar load csv --help
```

Replace csv with parquet to obtain Parquet specific options.