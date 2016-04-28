
# Spark Vector Connector (0.1)

A library to integrate Vector with Spark, allowing you to load Spark DataFrames/RDDs into Vector in parallel and to consume results of Vector based computations in Spark(SQL).
This connector works with both Vector SMP and VectorH MPP.

## API documentation

[Spark-Vector Connector](http://actiancorp.github.io/spark-vector/#com.actian.spark_vector.package) and [Spark-Vector Loader](http://actiancorp.github.io/spark-vector/loader/#com.actian.spark_vector.loader.package) Scaladocs.

## Requirements

This library requires:
* Vector(H) 4.2.3 (ONLY)
* Spark 1.5.x

## Building (from source)

Spark-Vector connector is built with [sbt](http://www.scala-sbt.org/). To build, run:

    sbt assembly

## Using with Spark shell/submit
This module can be added to Spark using the `--jars` command line option. Spark shell example (assuming `$SPARK_VECTOR` is the root directory of spark-vector):

    spark-shell --jars $SPARK_VECTOR/target/spark_vector-assembly-0.1.jar

Assuming that there is a Vector Installation on node `vectorhost`, instance `VI` and database `databasename`

### SparkSQL

```
sqlContext.sql("""CREATE TEMPORARY TABLE vector_table
USING com.actian.spark_vector.sql.DefaultSource
OPTIONS (
    host "vectorhost",
    instance "VI",
    database "databasename",
    table "vector_table"
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

The Spark-Vector loader is a command line client utility that provides the ability to load CSV,Parquet and ORC files through Spark into Vector, using the Spark-Vector connector.

#### Building

    sbt loader/assembly

#### Usage: CSV

Loading CSV files:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-0.1.jar load csv -sf hdfs://namenode:port/tmp/file.csv
-vh vectorhost -vi VI -vd databasename -tt vector_table -sc " "
```

#### Usage: Parquet

Loading Parquet files:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-0.1.jar load parquet -sf hdfs://namenode:port/tmp/file.parquet
-vh vectorhost -vi VI -vd databasename -tt vector_table
```

#### Usage: ORC

Loading ORC files:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-0.1.jar load orc -sf hdfs://namenode:port/tmp/file.orc
-vh vectorhost -vi VI -vd databasename -tt vector_table
```

#### List of options

The entire list of options is available [here](http://actiancorp.github.io/spark-vector/loader/#com.actian.spark_vector.loader.parsers.Args$) or can be retrieved with:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-0.1.jar load --help
```

## Unit testing

    sbt '; set javaOptions ++= "-Dvector.host=vectorhost -Dvector.instance=VI -Dvector.database=databasename -Dvector.user= -Dvector.password=".split(" ").toSeq; test'

### Spark-Vector Loader

    sbt loader/test
        
## License

Copyright 2016 Actian Corporation.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
