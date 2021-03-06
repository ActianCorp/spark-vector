
# Vector Data Source for Apache Spark

A library to integrate Vector with [Spark](https://spark.apache.org/), allowing you to load data from Spark into Vector in parallel and to consume the results of Vector based computations in Spark(SQL).
This connector works with both Vector SMP and VectorH MPP.

## API documentation

[Vector Data Source for Apache Spark](http://actiancorp.github.io/spark-vector/#com.actian.spark_vector.package) Scaladocs.

## Requirements

This library has different versions for Spark 1.5+ and 2.1+

| Spark Version | Compatible version of Vector Data Source for Spark |
| ------------- | -------------------------------------------------- |
| `1.5 - 1.6.3` | [`1.0`](https://github.com/ActianCorp/spark-vector/tree/1.0.x) |
| `2.1 - 2.3`   | [`2.0`](https://github.com/ActianCorp/spark-vector/tree/2.0.x) |
| `2.2+`        | `2.1`  (this version)                              |

This version also requires Vector(H) 5.0 or higher

## Building (from source)

The Vector data source for Apache Spark is built with [sbt](http://www.scala-sbt.org/). To build, run:

    sbt assembly

## Using with Spark shell/submit
This module can be added to Spark using the `--driver-class-path` command line option. Spark shell example (assuming `$SPARK_VECTOR` is the root directory of spark-vector):

    spark-shell --driver-class-path $SPARK_VECTOR/target/spark-vector-assembly-2.1-SNAPSHOT.jar

Assuming that there is a Vector installation on node `vectorhost`, instance `VI` and database `databasename`

### SparkSQL

```
spark.sqlContext.sql("""CREATE TEMPORARY VIEW vector_table
USING com.actian.spark_vector.sql.DefaultSource
OPTIONS (
    host "vectorhost",
    instance "VI",
    database "databasename",
    table "vector_table"
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

### Spark-Vector Loader

The Spark-Vector loader is a command line client utility that provides the ability to load CSV,Parquet and ORC files through Spark into Vector, using the Spark-Vector connector.

#### Building

    sbt loader/assembly
    
#### API documentation

[Loader scaladocs](http://actiancorp.github.io/spark-vector/loader/#com.actian.spark_vector.loader.package)

#### Usage: CSV

Loading CSV files into Vector with Spark:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.1-SNAPSHOT.jar load csv -sf hdfs://namenode:port/tmp/file.csv
-vh vectorhost -vi VI -vd databasename -tt vector_table -sc " "
```

#### Usage: Parquet

Loading Parquet files into Vector with Spark:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.1-SNAPSHOT.jar load parquet -sf hdfs://namenode:port/tmp/file.parquet
-vh vectorhost -vi VI -vd databasename -tt vector_table
```

#### Usage: ORC

Loading ORC files into Vector with Spark:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.1-SNAPSHOT.jar load orc -sf hdfs://namenode:port/tmp/file.orc
-vh vectorhost -vi VI -vd databasename -tt vector_table
```

#### List of options

The entire list of options is available [here](http://actiancorp.github.io/spark-vector/loader/#com.actian.spark_vector.loader.parsers.Args$) or can be retrieved with:

```
spark-submit --class com.actian.spark_vector.loader.Main $SPARK_VECTOR/loader/target/spark_vector_loader-assembly-2.1-SNAPSHOT.jar load --help
```

### Spark-Vector provider

The Spark-Vector provider is a Spark application serves Vector requests for external data sources.

#### Building

    sbt provider/assembly

#### API docs

[Provider scaladoc](http://actiancorp.github.io/spark-vector/provider/#com.actian.spark_vector.provider.package)


## Unit testing

    sbt '; set javaOptions ++= "-Dvector.host=vectorhost -Dvector.instance=VI -Dvector.database=databasename -Dvector.user= -Dvector.password=".split(" ").toSeq; test'

### Spark-Vector Loader

    sbt loader/test
        
## License

Copyright 2016 Actian Corporation.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
