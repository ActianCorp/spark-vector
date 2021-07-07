
# Vector Data Source for Apache Spark

A library to integrate Vector with [Spark](https://spark.apache.org/), allowing you to load data from Spark into Vector in parallel and to consume the results of Vector based computations in Spark(SQL).
This connector works with both Vector SMP and VectorH MPP. It is built with [sbt](http://www.scala-sbt.org/).

## Documentation
### Spark-Vector Data Source

    sbt doc
### Spark-Vector Loader

    sbt loader/doc
### Spark-Vector Provider

    sbt provider/doc

## Requirements

This library has different versions for Spark 1.5+, 2.1+ and 3.1.1.

| Spark Version | Compatible version of Vector Data Source for Spark |
| ------------- | -------------------------------------------------- |
| `1.5 - 1.6.3` | `1.0` |
| `2.1 - 2.3`   | `2.0` |
| `2.2+`        | `2.1` |
| `3.1.1`       | `3.0` |

## Spark-Vector Data Source
### Building

    sbt assembly

### Using with Spark shell/submit
This module can be added to Spark using the `--driver-class-path` command line option. Spark shell example (assuming `$SPARK_VECTOR` is the root directory of spark-vector):

    spark-shell --driver-class-path $SPARK_VECTOR/target/spark-vector-assembly-3.0.jar

Assuming that there is a Vector installation on node `vectorhost`, instance `VI` and database `databasename`

### SparkSQL

```
spark.sqlContext.sql("""CREATE TEMPORARY VIEW vector_table
USING com.actian.spark_vector.sql.DefaultSource
OPTIONS (
    host "vectorhost",
    instance "VI",
    database "databasename",
    table "vector_table",
    user "user",
    password "password"
)""")
```

and then to load data into Vector:

    spark.sqlContext.sql("insert into vector_table select * from spark_table")

... or to read Vector data into Spark:

    spark.sqlContext.sql("select * from vector_table")

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
    <td>Yes</td>
    <td>None</td>
   <td>User name to use when connecting to Vector</td>
 </tr>
 <tr>
    <td><tt>password</tt></td>
    <td>Yes</td>
    <td>None</td>
    <td>Password to use when connecting to Vector</td>
 </tr>
 <tr>
    <td><tt>table</tt></td>
    <td>Yes</td>
    <td>None</td>
    <td>Vector target table</td>
 </tr>
 <tr>
    <td><tt>loadpreSQL*</tt></td>
    <td>No</td>
    <td>None</td>
    <td>Query to execute before a load, in the same transaction. Multiple queries can be specified using different suffixes, e.g.  loadpreSQL0, loadpreSQL1, etc. In this case, the query execution order is determined by the lexicographic order
    </td>
 </tr>
 <tr>
    <td><tt>loadpostSQL*</tt></td>
    <td>No</td>
    <td>None</td>
    <td>Query to execute after a load, in the same transaction. Multiple queries can be specified using different suffixes, e.g.  loadpostSQL0, loadpostSQL1, etc. In this case, the query execution order is determined by the lexicographic order
    </td>
 </tr>
</table>

## Spark-Vector Loader

The Spark-Vector loader is a command line client utility that provides the ability to load CSV, Parquet and ORC files through Spark into Vector, using the Spark-Vector connector.

### Building

    sbt loader/assembly
### Usage: CSV

Loading CSV files into Vector with Spark:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-3.0.jar load csv -sf hdfs://namenode:port/tmp/file.csv
-vh vectorhost -vi VI -vd databasename -tt vector_table -sc " " -vu <user> -vp <password>
```

### Usage: Parquet

Loading Parquet files into Vector with Spark:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-3.0.jar load parquet -sf hdfs://namenode:port/tmp/file.parquet
-vh vectorhost -vi VI -vd databasename -tt vector_table -vu <user> -vp <password>
```

### Usage: ORC

Loading ORC files into Vector with Spark:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-3.0.jar load orc -sf hdfs://namenode:port/tmp/file.orc
-vh vectorhost -vi VI -vd databasename -tt vector_table -vu <user> -vp <password>
```

### List of options

A full list of of options can be retrieved with:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-3.0.jar load --help
```

## Spark-Vector provider

The Spark-Vector provider is a Spark application which serves Vector requests for external data sources. Documentation can be found [here](https://docs.actian.com/vector/6.0/index.html#page/User%2F14._Using_External_Tables.htm%23ww418048).

### Building

    sbt provider/assembly

## Unit testing
### Spark-Vector Data Source

    sbt '; set javaOptions ++= "-Dvector.host=<host> -Dvector.instance=<instance ID> -Dvector.database=<database> -Dvector.user=<user> -Dvector.password=<password> -Dprovider.sparkHome=<path to spark distribution> -Dprovider.jar=<path to spark_vector_provider assembly jar> -Dprovider.sparkInfoFile=<path to spark_info_file shared with Vector>".split(" ").toSeq; test'

### Spark-Vector Loader

    sbt loader/test

### Spark-Vector Provider

    sbt provider/test
## License

Copyright 2021 Actian Corporation.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
