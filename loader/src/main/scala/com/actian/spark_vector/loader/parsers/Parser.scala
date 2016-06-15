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
package com.actian.spark_vector.loader.parsers

import com.actian.spark_vector.loader.options._
import com.actian.spark_vector.vector.VectorConnectionProperties

import scopt.{ OptionDef, Read }

sealed class ArgDescription(val longName: String, val shortName: String, val description: String)

/**
 * Option describing a command line argument.
 *
 *  @param lName Name for argument
 *  @param sName Short name for argument (two letters)
 *  @param desc Description of argument
 *  @param extractor Extracts the value of the argument from the global [[options.UserOptions UserOptions]]
 *  @param injector Updates a [[options.UserOptions UserOptions]] instance, with an updated value for this argument
 *  @param mandatory Flag to describe if the argument is required or not
 */
sealed case class ArgOption[T: Read, O](
    lName: String,
    sName: String,
    desc: String,
    extractor: UserOptions => O,
    injector: (T, UserOptions) => UserOptions,
    mandatory: Boolean) extends ArgDescription(lName, sName, desc) {
  def asOpt(parser: scopt.OptionParser[UserOptions]): OptionDef[T, UserOptions] =
    parser.opt[T](longName) abbr (shortName) action (injector) text (description)
}

/**
 * Object where all command line arguments are defined:
 *
 * {{{
 * Spark Vector load tool 1.1.0
 * Usage: spark-submit --class com.actian.spark_vector.loader.Main <spark_vector_loader-assembly-1.1-SNAPSHOT.jar> [load] [options]
 *
 * Spark Vector load
 *   --help
 *         This tool can be used to load CSV/Parquet/ORC files through Spark to Vector
 * Command: load [csv|parquet|orc]
 * Read a file and load into Vector
 * Command: load csv [options]
 * Load a csv file
 *   -sf <value> | --sourceFile <value>
 *         Source file
 *   -cols <value> | --cols <value>
 *         Comma separated string containing only column names to load
 *   -vh <value> | --vectorHost <value>
 *         Vector host name
 *   -vi <value> | --vectorInstance <value>
 *         Vector instance
 *   -vd <value> | --vectorDatabase <value>
 *         Vector database
 *   -vu <value> | --vectorUser <value>
 *         Vector user
 *   -vp <value> | --vectorPass <value>
 *         Vector password
 *   -tt <value> | --vectorTargetTable <value>
 *         Vector target table
 *   -preSQL <value> | --preSQL <value>
 *         Queries to execute in Vector before loading, separated by ';'
 *   -postSQL <value> | --postSQL <value>
 *         Queries to execute in Vector after loading, separated by ';'
 *   -sh <value> | --skipHeader <value>
 *         Skip header row
 *   -en <value> | --encoding <value>
 *         CSV text encoding
 *   -np <value> | --nullPattern <value>
 *         CSV null pattern
 *   -sc <value> | --separatorChar <value>
 *         CSV field separator character
 *   -qc <value> | --quoteChar <value>
 *         CSV quote character
 *   -ec <value> | --escapeChar <value>
 *         CSV escape character
 *   -h <value> | --header <value>
 *         Comma separated string with CSV column names and datatypes, e.g. "col1 int, col2 string,"
 *   -pl <value> | --parserLib <value>
 *         CSV parser library (for spark-csv): either default or univocity
 * Command: load parquet [options]
 * Load a parquet file
 *   -sf <value> | --sourceFile <value>
 *         Source file
 *   -cols <value> | --cols <value>
 *         Comma separated string containing only column names to load
 *   -vh <value> | --vectorHost <value>
 *         Vector host name
 *   -vi <value> | --vectorInstance <value>
 *         Vector instance
 *   -vd <value> | --vectorDatabase <value>
 *         Vector database
 *   -vu <value> | --vectorUser <value>
 *         Vector user
 *   -vp <value> | --vectorPass <value>
 *         Vector password
 *   -tt <value> | --vectorTargetTable <value>
 *         Vector target table
 *   -preSQL <value> | --preSQL <value>
 *         Queries to execute in Vector before loading, separated by ';'
 *   -postSQL <value> | --postSQL <value>
 *         Queries to execute in Vector after loading, separated by ';'
 * Command: load orc [options]
 * Load an orc file
 *   -sf <value> | --sourceFile <value>
 *         Source file
 *   -cols <value> | --cols <value>
 *         Comma separated string containing only column names to load
 *   -vh <value> | --vectorHost <value>
 *         Vector host name
 *   -vi <value> | --vectorInstance <value>
 *         Vector instance
 *   -vd <value> | --vectorDatabase <value>
 *         Vector database
 *   -vu <value> | --vectorUser <value>
 *         Vector user
 *   -vp <value> | --vectorPass <value>
 *         Vector password
 *   -tt <value> | --vectorTargetTable <value>
 *         Vector target table
 *   -preSQL <value> | --preSQL <value>
 *         Queries to execute in Vector before loading, separated by ';'
 *   -postSQL <value> | --postSQL <value>
 *         Queries to execute in Vector after loading, separated by ';'
 * }}}
 */
object Args {
  import UserOptions._
  private implicit object ReadChar extends Read[Char] {
    override def arity: Int = 1

    override def reads: (String) => Char = _.head
  }

  private implicit object ReadSeqString extends Read[Seq[String]] {
    override def arity: Int = 1
    override def reads: (String) => Seq[String] = _.split(";").toSeq
  }

  val load = new ArgDescription("load", "lh", "Read a file and load into Vector")
  val csvLoad = new ArgDescription("csv", "csv", "Load a csv file")
  val parquetLoad = new ArgDescription("parquet", "parquet", "Load a parquet file")
  val orcLoad = new ArgDescription("orc", "orc", "Load an orc file")

  val vectorHost = ArgOption[String, String](
    "vectorHost", "vh", "Vector host name", _.vector.host, updateVector((o, v) => o.copy(host = v)), true)
  val vectorInstance = ArgOption[String, String](
    "vectorInstance", "vi", "Vector instance", _.vector.instance, updateVector((o, v) => o.copy(instance = v)), true)
  val vectorDatabase = ArgOption[String, String](
    "vectorDatabase", "vd", "Vector database", _.vector.database, updateVector((o, v) => o.copy(database = v)), true)
  val vectorUser = ArgOption[String, Option[String]](
    "vectorUser", "vu", "Vector user", _.vector.user, updateVector((o, v) => o.copy(user = Some(v))), false)
  val vectorPassword = ArgOption[String, Option[String]](
    "vectorPass", "vp", "Vector password", _.vector.password, updateVector((o, v) => o.copy(password = Some(v))), false)
  val vectorTargetTable = ArgOption[String, String](
    "vectorTargetTable", "tt", "Vector target table", _.vector.targetTable, updateVector((o, v) => o.copy(targetTable = v)), true)
  val preSQL = ArgOption[Seq[String], Option[Seq[String]]](
    "preSQL", "preSQL", "Queries to execute in Vector before loading, separated by ';'", _.vector.preSQL, updateVector((o, v) => o.copy(preSQL = Some(v))), false)
  val postSQL = ArgOption[Seq[String], Option[Seq[String]]](
    "postSQL", "postSQL", "Queries to execute in Vector after loading, separated by ';'", _.vector.postSQL, updateVector((o, v) => o.copy(postSQL = Some(v))), false)

  val vectorArgs = Seq(vectorHost, vectorInstance, vectorDatabase, vectorUser, vectorPassword, vectorTargetTable, preSQL, postSQL)

  val inputFile = ArgOption[String, String](
    "sourceFile", "sf", "Source file", _.general.sourceFile, updateGeneral((o, v) => o.copy(sourceFile = v.replace('\\', '/'))), true)
  val colsToLoad = ArgOption[String, Option[Seq[_]]](
    "cols", "cols", "Comma separated string containing only column names to load", _.general.colsToLoad, updateGeneral((o, v) => o.copy(colsToLoad = Some(v.split(",")))), false)

  val generalArgs = Seq(inputFile, colsToLoad)

  val hRow = ArgOption[Boolean, Option[Boolean]](
    "skipHeader", "sh", "Skip header row", _.csv.headerRow, updateCSV((o, v) => o.copy(headerRow = Option(v))), false)
  val encoding = ArgOption[String, Option[String]](
    "encoding", "en", "CSV text encoding", _.csv.encoding, updateCSV((o, v) => o.copy(encoding = Option(v))), false)
  val nullPattern = ArgOption[String, Option[String]](
    "nullPattern", "np", "CSV null pattern", _.csv.nullPattern, updateCSV((o, v) => o.copy(nullPattern = Option(v))), false)
  val separatorChar = ArgOption[Char, Option[Char]](
    "separatorChar", "sc", "CSV field separator character", _.csv.separatorChar, updateCSV((o, v) => o.copy(separatorChar = Option(v))), false)
  val quoteChar = ArgOption[Char, Option[Char]](
    "quoteChar", "qc", "CSV quote character", _.csv.quoteChar, updateCSV((o, v) => o.copy(quoteChar = Option(v))), false)
  val escapeChar = ArgOption[Char, Option[Char]](
    "escapeChar", "ec", "CSV escape character", _.csv.escapeChar, updateCSV((o, v) => o.copy(escapeChar = Option(v))), false)
  val header = ArgOption[String, Option[Seq[_]]](
    "header", "h", "Comma separated string with CSV column names and datatypes, e.g. \"col1 int, col2 string,\"", _.csv.header, updateCSV((o, v) => o.copy(header = Some(v.split(",")))), false)
  val parserLib = ArgOption[String, Option[String]](
    "parserLib", "pl", "CSV parser library (for spark-csv): either default or univocity", _.csv.nullPattern, updateCSV((o, v) => o.copy(parserLib = Option(v))), false)

  val csvArgs = Seq(hRow, encoding, nullPattern, separatorChar, quoteChar, escapeChar, header, parserLib)
  val parquetArgs = Seq.empty[ArgOption[_, _]]
  val orcArgs = Seq.empty[ArgOption[_, _]]

  val csvOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ csvArgs
  val parquetOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ parquetArgs
  val orcOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ orcArgs

  val modeToOptions = Map(csvLoad.longName -> csvOptions, parquetLoad.longName -> parquetOptions, orcLoad.longName -> orcOptions)
}

object Parser extends scopt.OptionParser[UserOptions]("spark-submit --class com.actian.spark_vector.loader.Main <spark_vector_loader-assembly-1.1-SNAPSHOT.jar>") {
  import Args._

  head("Spark Vector load tool", "1.1.0")
  note("Spark Vector load")
  help("help").text("This tool can be used to load CSV/Parquet/ORC files through Spark to Vector")
  cmd(load.longName)
    .action((_, options) => options.copy(mode = load.longName))
    .abbr(load.shortName)
    .text(load.description)
    .children(
      cmd(csvLoad.longName)
        .action((_, options) => options.copy(mode = csvLoad.longName))
        .abbr(csvLoad.shortName)
        .text(csvLoad.description)
        .children(csvOptions.map(_.asOpt(this)): _*),
      cmd(parquetLoad.longName)
        .action((_, options) => options.copy(mode = parquetLoad.longName))
        .abbr(parquetLoad.shortName)
        .text(parquetLoad.description)
        .children(parquetOptions.map(_.asOpt(this)): _*),
      cmd(orcLoad.longName)
        .action((_, options) => options.copy(mode = orcLoad.longName))
        .abbr(orcLoad.shortName)
        .text(orcLoad.description)
        .children(orcOptions.map(_.asOpt(this)): _*))

  checkConfig { options =>
    if (options.mode.isEmpty || options.mode == load.longName) {
      failure(s"Invalid command. Available commands are ${modeToOptions.keys.map(mode => s"load $mode").mkString(",")}")
    } else {
      validateOptions(options)
    }
  }

  private def validateOptions(options: UserOptions): Either[String, Unit] = {
    // TODO: Risky assumption. Fix when the first non-string required configuration option occurs.
    val required = modeToOptions(options.mode).filter(_.mandatory).map(_.asInstanceOf[ArgOption[_, String]])
    val missing =
      required.foldLeft(Seq.empty[String])((acc, cur) => {
        if (cur.extractor(options).isEmpty) acc :+ cur.longName else acc
      })
    if (missing.isEmpty) {
      success
    } else {
      failure("Errors are: " + missing.map(n => s"${n} missing").mkString(";"))
    }
  }
}
