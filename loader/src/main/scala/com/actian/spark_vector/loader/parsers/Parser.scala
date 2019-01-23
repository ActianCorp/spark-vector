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
 * Spark Vector load tool 2.0.0
 * Usage: spark-submit --class com.actian.spark_vector.loader.Main <spark_vector_loader-assembly-2.1.jar> [load] [options]
 *
 * Spark Vector load
 *   --help
 *         This tool can be used to load CSV/Parquet/ORC files through Spark to Vector
 *         
 * Command: load [csv|parquet|orc]
 * Read a file and load into Vector
 * 
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
 *   -vo <value> | --vectorPort <value>
 *   			 Vector port
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
 *         
 *   -h <value> | --header <value>
 *         Comma separated string with CSV column names and datatypes, e.g. "col1 int, col2 string,"
 *   -sh <value> | --skipHeader <value>
 *         Skip header row so it is not included as data (default false)
 *   -is <value> | --inferSchema <value>
 *         Infer CSV input schema automatically (default false)
 *   -en <value> | --encoding <value>
 *         CSV text encoding type (default UTF-8)
 *   -sc <value> | --separatorChar <value>
 *         CSV field separator character (default ,)
 *   -qc <value> | --quoteChar <value>
 *         CSV quote character (default "")
 *   -ec <value> | --escapeChar <value>
 *         CSV escape character (default \)
 *   -cc <value> | --commentChar <value>
 *         CSV comment character, disabled by default
 *   -il <value> | --ignoreLeadingWS <value>
 *         Whether leading whitespaces on values should be skipped (default false)
 *   -it <value> | --ignoreTrailingWS <value>
 *         Whether trailing whitespaces on values should be skipped (default false)
 *   -nv <value> | --nullValue <value>
 *         String representation of a null value (default empty string)
 *   -nnv <value> | --nanValue <value>
 *         String representation of a non-number value (default NaN)
 *   -pi <value> | --positiveInf <value>
 *         String representation of a positive infinity value (default Inf)
 *   -ni <value> | --negativeInf <value>
 *         String representation of a negativ infinity value (default -Inf)
 *   -df <value> | --dateFormat <value>
 *         CSV date format string, custom date formats follow the formats at java.text.SimpleDateFormat (default yyyy-MM-dd)
 *   -tf <value> | --timestampFormat <value>
 *         CSV timestamp format string, custom timestamp formats follow the formats at java.text.SimpleDateFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSZZ)
 *   -pm <value> | --parseMode <value>
 *         Set parse mode for dealing with corrupt records during parsing (default PERMISSIVE)
 *         PERMISSIVE - sets other fields to null in corrupted records
 *         DROPMALFORMED - ignore corrupted records
 *         FAILFAST - throws an exception on corrupted record
 *         
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
 *   -vo <value> | --vectorPort <value>
 *   			 Vector port
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
 *         
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
 *   -vo <value> | --vectorPort <value>
 *   			 Vector port
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

  // Vector arguments
  val vectorHost = ArgOption[String, String](
    "vectorHost", "vh", "Vector host name", _.vector.host, updateVector((o, v) => o.copy(host = v)), true)
  val vectorInstance = ArgOption[String, String](
    "vectorInstance", "vi", "Vector instance", _.vector.instance, updateVector((o, v) => o.copy(instance = v)), true)
  val vectorDatabase = ArgOption[String, String](
    "vectorDatabase", "vd", "Vector database", _.vector.database, updateVector((o, v) => o.copy(database = v)), true)
  val vectorPort = ArgOption[String, String](
    "vectorPort", "vo", "Vector port", _.vector.port, updateVector((o, v) => o.copy(port = v)), false)
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

  val vectorArgs = Seq(vectorHost, vectorInstance, vectorDatabase, vectorPort, vectorUser, vectorPassword, vectorTargetTable, preSQL, postSQL)

  // Generic arguments
  val inputFile = ArgOption[String, String](
    "sourceFile", "sf", "Source file", _.general.sourceFile, updateGeneral((o, v) => o.copy(sourceFile = v.replace('\\', '/'))), true)
  val colsForLoad = ArgOption[String, Option[Seq[_]]](
    "cols", "cols", "Comma separated string containing only column names to load", _.general.colsToLoad, updateGeneral((o, v) => o.copy(colsToLoad = Some(v.split(",").map(_.trim())))), false)

  val generalArgs = Seq(inputFile, colsForLoad)

  // CSV arguments
  val hRow = ArgOption[Boolean, Option[Boolean]](
    "skipHeader", "sh", "Skip header row", _.csv.headerRow, updateCSV((o, v) => o.copy(headerRow = Option(v))), false)
  val inferSchema = ArgOption[Boolean, Option[Boolean]](
    "inferSchema", "is", "Infer the CSV input schema automatically", _.csv.inferSchema, updateCSV((o, v) => o.copy(inferSchema = Option(v))), false)
  val encode = ArgOption[String, Option[String]](
    "encoding", "en", "CSV text encoding", _.csv.encoding, updateCSV((o, v) => o.copy(encoding = Option(v))), false)
  val separatorChar = ArgOption[Char, Option[Char]](
    "separatorChar", "sc", "CSV field separator character", _.csv.separatorChar, updateCSV((o, v) => o.copy(separatorChar = Option(v))), false)
  val quoteChar = ArgOption[Char, Option[Char]](
    "quoteChar", "qc", "CSV quote character", _.csv.quoteChar, updateCSV((o, v) => o.copy(quoteChar = Option(v))), false)
  val escapeChar = ArgOption[Char, Option[Char]](
    "escapeChar", "ec", "CSV escape character", _.csv.escapeChar, updateCSV((o, v) => o.copy(escapeChar = Option(v))), false)
  val commentChar = ArgOption[Char, Option[Char]](
    "commentChar", "cc", "CSV comment character", _.csv.commentChar, updateCSV((o, v) => o.copy(commentChar = Option(v))), false)
  val ignoreLeading = ArgOption[Boolean, Option[Boolean]](
    "ignoreLeadingWS", "il", " Whether leading whitespaces on values are skipped", _.csv.ignoreLeading, updateCSV((o, v) => o.copy(ignoreLeading = Option(v))), false)
  val ignoreTrailing = ArgOption[Boolean, Option[Boolean]](
    "ignoreTrailingWS", "it", "Whether trailing whitespaces on values are skipped", _.csv.ignoreTrailing, updateCSV((o, v) => o.copy(ignoreTrailing = Option(v))), false) 
  val nullValue = ArgOption[String, Option[String]](
    "nullValue", "nv", "String representation of a null value", _.csv.nullValue, updateCSV((o, v) => o.copy(nullValue =  Option(v))), false)
  val nanValue = ArgOption[String, Option[String]](
    "nanValue", "nnv", "String representation of a non-number value", _.csv.nanValue, updateCSV((o, v) => o.copy(nanValue = Option(v))), false)
  val positiveInf = ArgOption[String, Option[String]](
    "positiveInf", "pi", "String representation of a positive infinity value", _.csv.positiveInf, updateCSV((o, v) => o.copy(positiveInf = Option(v))), false)
  val negativeInf = ArgOption[String, Option[String]](
    "negativeInf", "ni", "String representation of a negative infinity value", _.csv.negativeInf, updateCSV((o, v) => o.copy(negativeInf = Option(v))), false)
  val dateFormat = ArgOption[String, Option[String]](
    "dateFormat", "df", "CSV date format string", _.csv.dateFormat, updateCSV((o, v) => o.copy(dateFormat = Option(v))), false)
  val timestampFormat = ArgOption[String, Option[String]](
    "timestampFormat", "tf", "CSV timestamp format string", _.csv.timestampFormat, updateCSV((o, v) => o.copy(timestampFormat = Option(v))), false)
  val parseMode = ArgOption[String, Option[String]](
    "parseMode", "pm", "Set parse mode for dealing with corrupt records during parsing", _.csv.parseMode, updateCSV((o, v) => o.copy(parseMode = Option(v))), false)
  val header = ArgOption[String, Option[Seq[_]]](
    "header", "h", "Comma separated string with CSV column names and datatypes, e.g. \"col1 int, col2 string,\"", _.csv.header, updateCSV((o, v) => o.copy(header = Some(v.split(",")))), false)
 
  val csvArgs = Seq(header, hRow, inferSchema, encode, separatorChar, quoteChar, escapeChar, commentChar, 
    ignoreLeading, ignoreTrailing, nullValue, nanValue, positiveInf, negativeInf, dateFormat, timestampFormat, parseMode)
  val parquetArgs = Seq.empty[ArgOption[_, _]]
  val orcArgs = Seq.empty[ArgOption[_, _]]

  val csvOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ csvArgs
  val parquetOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ parquetArgs
  val orcOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ orcArgs

  val modeToOptions = Map(csvLoad.longName -> csvOptions, parquetLoad.longName -> parquetOptions, orcLoad.longName -> orcOptions)
}

object Parser extends scopt.OptionParser[UserOptions]("spark-submit --class com.actian.spark_vector.loader.Main <spark_vector_loader-assembly-2.1.jar>") {
  import Args._

  head("Spark Vector load tool", "2.0.0")
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
