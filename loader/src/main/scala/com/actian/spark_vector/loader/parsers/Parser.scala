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
 *
 * Maintainer: francis.gropengieser@actian.com
 */
package com.actian.spark_vector.loader.parsers

import com.actian.spark_vector.BuildInfo
import com.actian.spark_vector.loader.options._
import com.actian.spark_vector.vector.JDBCPort

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
 * Usage: spark-submit --class com.actian.spark_vector.loader.Main <spark_vector_loader-assembly-2.1.jar> load [csv|parquet|orc|json] [common options] [format specific options]
 *
 * Spark Vector load
 *   --help
 *         This tool can be used to load CSV/Parquet/ORC files through Spark to Vector
 *
 * Command: load [csv|parquet|orc|json] [common options] [format specific options]
 * Read a file and load into Vector
 *
 * Common options:
 *
 *   -sf <value> | --sourceFile <value>
 *         Source file
 *   -cols <value> | --cols <value>
 *         Comma separated string containing only column names to load
 *   -ct <value> | --createTable <value>
 *         Whether the table should be created (default false)
 *   -vh <value> | --vectorHost <value>
 *         Vector host name
 *   -vi <value> | --vectorInstance <value>
 *         Vector instance
 *   -vo <value> | --vectorInstOffset <value>
 *         Vector JDBC instance offset
 *   -vj <value> | --vectorJDBCPort <value>
 *         Vector JDBC port
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
 *
 * CSV specific options:
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
 * JSON specific options:
 *
 *   -h <value> | --header <value>
 *         Comma separated string with JSON column names and datatypes, e.g. "col1 int, col2 string,"
 *   -ps <value> | --primitivesAsString <value>
 *         Infer all primitve values as a string type (default false)
 *   -ac <value> | --allowComments <value>
 *         Ignores Java/C++ style comments in JSON records (default false)
 *   -au <value> | --allowUnquoted <value>
 *         Allow unquoted JSON field names in the record (default false)
 *   -as <value> | --allowSingleQuotes
 *         Allow single quotes in addition to double quotes (default false)
 *   -al <value> | --allowLeadingZeros
 *         Allow leading zeros in numbers (default false)
 *   -ae <value> | --allowEscapingAny
 *         Allow accepting quoting of all characters using backslash quoting mechanism (default false)
 *   -ml <value> | --multiline
 *         Parse one record, which may span multiple lines, per file (default false)
 *   -pm <value> | --parseMode <value>
 *         Set parse mode for dealing with corrupt records during parsing (default PERMISSIVE)
 *         PERMISSIVE - sets other fields to null in corrupted records
 *         DROPMALFORMED - ignore corrupted records
 *         FAILFAST - throws an exception on corrupted record
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
  val jsonLoad = new ArgDescription("json", "json", "Load a json file")
  // Vector arguments
  val vectorHost = ArgOption[String, String](
    "vectorHost", "vh", "Vector host name", _.vector.host, updateVector((o, v) => o.copy(host = v)), true)
  val vectorInstance = ArgOption[String, Option[String]](
    "vectorInstance", "vi", "Vector instance", _.vector.instance, updateVector((o, v) => o.copy(instance = Some(v))), false)
  val vectorInstOffset = ArgOption[String, Option[String]](
    "vectorInstOffset", "vo", "Vector JDBC instance offset", _.vector.instanceOffset, updateVector((o, v) => o.copy(instanceOffset = Some(v))), false)
  val vectorJDBCPort = ArgOption[String, Option[String]](
    "vectorJDBCPort", "vj", "Vector JDBC port", _.vector.jdbcPort, updateVector((o, v) => o.copy(jdbcPort = Some(v))), false)
  val vectorDatabase = ArgOption[String, String](
    "vectorDatabase", "vd", "Vector database", _.vector.database, updateVector((o, v) => o.copy(database = v)), true)
  val vectorUser = ArgOption[String, Option[String]](
    "vectorUser", "vu", "Vector user", _.vector.user, updateVector((o, v) => o.copy(user = Some(v))), false)
  val vectorPassword = ArgOption[String, Option[String]](
    "vectorPass", "vp", "Vector password", _.vector.password, updateVector((o, v) => o.copy(password = Some(v))), false)
  val vectorTargetTable = ArgOption[String, String](
    "vectorTargetTable", "tt", "Vector target table", _.vector.targetTable, updateVector((o, v) => o.copy(targetTable = v)), true)
  val vectorPreSQL = ArgOption[Seq[String], Option[Seq[String]]](
    "preSQL", "preSQL", "Queries to execute in Vector before loading, separated by ';'", _.vector.preSQL, updateVector((o, v) => o.copy(preSQL = Some(v))), false)
  val vectorPostSQL = ArgOption[Seq[String], Option[Seq[String]]](
    "postSQL", "postSQL", "Queries to execute in Vector after loading, separated by ';'", _.vector.postSQL, updateVector((o, v) => o.copy(postSQL = Some(v))), false)

  val vectorArgs = Seq(vectorHost, vectorInstance, vectorInstOffset, vectorJDBCPort, vectorDatabase, vectorUser, vectorPassword, vectorTargetTable, vectorPreSQL, vectorPostSQL)

  // Generic arguments
  val genInputFile = ArgOption[String, String](
    "sourceFile", "sf", "Source file", _.general.sourceFile, updateGeneral((o, v) => o.copy(sourceFile = v.replace('\\', '/'))), true)
  val genColsForLoad = ArgOption[String, Option[Seq[_]]](
    "cols", "cols", "Comma separated string containing only column names to load", _.general.colsToLoad, updateGeneral((o, v) => o.copy(colsToLoad = Some(v.split(",").map(_.trim())))), false)
  val genCreateTable = ArgOption[Boolean, Option[Boolean]](
    "createTable", "ct", "Whether the table should be created", _.general.createTable, updateGeneral((o, v) => o.copy(createTable = Option(v))), false)
  val generalArgs = Seq(genInputFile, genColsForLoad, genCreateTable)

  // CSV arguments
  val csvHRow = ArgOption[Boolean, Option[Boolean]](
    "skipHeader", "sh", "Skip header row", _.csv.headerRow, updateCSV((o, v) => o.copy(headerRow = Option(v))), false)
  val csvInferSchema = ArgOption[Boolean, Option[Boolean]](
    "inferSchema", "is", "Infer the CSV input schema automatically", _.csv.inferSchema, updateCSV((o, v) => o.copy(inferSchema = Option(v))), false)
  val csvEncode = ArgOption[String, Option[String]](
    "encoding", "en", "CSV text encoding", _.csv.encoding, updateCSV((o, v) => o.copy(encoding = Option(v))), false)
  val csvSeparatorChar = ArgOption[Char, Option[Char]](
    "separatorChar", "sc", "CSV field separator character", _.csv.separatorChar, updateCSV((o, v) => o.copy(separatorChar = Option(v))), false)
  val csvQuoteChar = ArgOption[Char, Option[Char]](
    "quoteChar", "qc", "CSV quote character", _.csv.quoteChar, updateCSV((o, v) => o.copy(quoteChar = Option(v))), false)
  val csvEscapeChar = ArgOption[Char, Option[Char]](
    "escapeChar", "ec", "CSV escape character", _.csv.escapeChar, updateCSV((o, v) => o.copy(escapeChar = Option(v))), false)
  val csvCommentChar = ArgOption[Char, Option[Char]](
    "commentChar", "cc", "CSV comment character", _.csv.commentChar, updateCSV((o, v) => o.copy(commentChar = Option(v))), false)
  val csvIgnoreLeading = ArgOption[Boolean, Option[Boolean]](
    "ignoreLeadingWS", "il", "Whether leading whitespaces on values are skipped", _.csv.ignoreLeading, updateCSV((o, v) => o.copy(ignoreLeading = Option(v))), false)
  val csvIgnoreTrailing = ArgOption[Boolean, Option[Boolean]](
    "ignoreTrailingWS", "it", "Whether trailing whitespaces on values are skipped", _.csv.ignoreTrailing, updateCSV((o, v) => o.copy(ignoreTrailing = Option(v))), false)
  val csvNullValue = ArgOption[String, Option[String]](
    "nullValue", "nv", "String representation of a null value", _.csv.nullValue, updateCSV((o, v) => o.copy(nullValue =  Option(v))), false)
  val csvNanValue = ArgOption[String, Option[String]](
    "nanValue", "nnv", "String representation of a non-number value", _.csv.nanValue, updateCSV((o, v) => o.copy(nanValue = Option(v))), false)
  val csvPositiveInf = ArgOption[String, Option[String]](
    "positiveInf", "pi", "String representation of a positive infinity value", _.csv.positiveInf, updateCSV((o, v) => o.copy(positiveInf = Option(v))), false)
  val csvNegativeInf = ArgOption[String, Option[String]](
    "negativeInf", "ni", "String representation of a negative infinity value", _.csv.negativeInf, updateCSV((o, v) => o.copy(negativeInf = Option(v))), false)
  val csvDateFormat = ArgOption[String, Option[String]](
    "dateFormat", "df", "CSV date format string", _.csv.dateFormat, updateCSV((o, v) => o.copy(dateFormat = Option(v))), false)
  val csvTimestampFormat = ArgOption[String, Option[String]](
    "timestampFormat", "tf", "CSV timestamp format string", _.csv.timestampFormat, updateCSV((o, v) => o.copy(timestampFormat = Option(v))), false)
  val csvParseMode = ArgOption[String, Option[String]](
    "parseMode", "pm", "Set parse mode for dealing with corrupt records during parsing", _.csv.parseMode, updateCSV((o, v) => o.copy(parseMode = Option(v))), false)
  val csvHeader = ArgOption[String, Option[Seq[_]]](
    "header", "h", "Comma separated string with CSV column names and datatypes, e.g. \"col1 int, col2 string,\"", _.csv.header, updateCSV((o, v) => o.copy(header = Some(v.split(",")))), false)

    // JSON arguments
  val jsonPrimitivesAsStr = ArgOption[Boolean, Option[Boolean]](
    "primitivesAsString", "ps", "Infer all primitive values as a string type", _.json.primitivesAsString, updateJSON((o, v) => o.copy(primitivesAsString = Option(v))), false)
  val jsonAllowComments = ArgOption[Boolean, Option[Boolean]](
    "allowComments", "ac", "Ignores Java/C++ style comments in JSON records", _.json.allowComments, updateJSON((o, v) => o.copy(allowComments = Option(v))), false)
  val jsonAllowUnquoted = ArgOption[Boolean, Option[Boolean]](
    "allowUnquoted", "ac", "Allow unquoted JSON field names in the record", _.json.allowUnquoted, updateJSON((o, v) => o.copy(allowUnquoted = Option(v))), false)
  val jsonAllowSingleQuotes = ArgOption[Boolean, Option[Boolean]](
    "allowSingleQuotes", "as", "Allow single quotes in addition to double quotes", _.json.allowSingleQuotes, updateJSON((o, v) => o.copy(allowSingleQuotes = Option(v))), false)
  val jsonAllowLeadingZeros = ArgOption[Boolean, Option[Boolean]](
    "allowLeadingZeros", "as", "Allow leading zeros in numbers", _.json.allowLeadingZeros, updateJSON((o, v) => o.copy(allowLeadingZeros = Option(v))), false)
  val jsonAllowEscapingAny = ArgOption[Boolean, Option[Boolean]](
    "allowEscapingAny", "as", "Allow accepting quoting of all characters using backslash quoting mechanism", _.json.allowEscapingAny, updateJSON((o, v) => o.copy(allowEscapingAny = Option(v))), false)
  val jsonAllowUnqCtrlChars = ArgOption[Boolean, Option[Boolean]](
    "allowUnquotedControlChars", "auc", "Whether unquoted control characters are allowed in strings", _.json.allowUnquotedControlChars, updateJSON((o, v) => o.copy(allowUnquotedControlChars = Option(v))), false)
  val jsonMultiline = ArgOption[Boolean, Option[Boolean]](
    "multiline", "ml", "Parse one record, which may span multiple lines, per file", _.json.multiline, updateJSON((o, v) => o.copy(multiline = Option(v))), false)
  val jsonParseModeJson = ArgOption[String, Option[String]](
    "parseMode", "pm", "Set parse mode for dealing with corrupt records during parsing", _.json.parseMode, updateJSON((o, v) => o.copy(parseMode = Option(v))), false)
  val jsonHeaderJson = ArgOption[String, Option[Seq[_]]](
    "header", "h", "Comma separated string with JSON column names and datatypes, e.g. \"col1 int, col2 string,\"", _.json.header, updateJSON((o, v) => o.copy(header = Some(v.split(",")))), false)

  val csvArgs = Seq(csvHeader, csvHRow, csvInferSchema, csvEncode, csvSeparatorChar, csvQuoteChar, csvEscapeChar, csvCommentChar,
    csvIgnoreLeading, csvIgnoreTrailing, csvNullValue, csvNanValue, csvPositiveInf, csvNegativeInf, csvDateFormat, csvTimestampFormat, csvParseMode)
  val jsonArgs = Seq(jsonHeaderJson, jsonPrimitivesAsStr, jsonAllowComments, jsonAllowUnquoted, jsonAllowSingleQuotes, jsonAllowLeadingZeros, jsonAllowEscapingAny, jsonAllowUnqCtrlChars, jsonMultiline, jsonParseModeJson)
  val parquetArgs = Seq.empty[ArgOption[_, _]]
  val orcArgs = Seq.empty[ArgOption[_, _]]

  val csvOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ csvArgs
  val parquetOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ parquetArgs
  val orcOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ orcArgs
  val jsonOptions: Seq[ArgOption[_, _]] = generalArgs ++ vectorArgs ++ jsonArgs
  val modeToOptions = Map(
      csvLoad.longName -> csvOptions,
      parquetLoad.longName -> parquetOptions,
      orcLoad.longName -> orcOptions,
      jsonLoad.longName -> jsonOptions)
}

object Parser extends scopt.OptionParser[UserOptions](s"""spark-submit --class com.actian.spark_vector.loader.Main <spark_vector_loader-assembly-${BuildInfo.version}.jar>""") {
  import Args._

  head("Spark Vector load tool", BuildInfo.version)
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
        .children(orcOptions.map(_.asOpt(this)): _*),
      cmd(jsonLoad.longName)
        .action((_, options) => options.copy(mode = jsonLoad.longName))
        .abbr(jsonLoad.shortName)
        .text(jsonLoad.description)
        .children(jsonOptions.map(_.asOpt(this)): _*))

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
        val instance = JDBCPort.instanceRegex.r
        val offset = JDBCPort.offsetOrPortRegex.r
        val port = JDBCPort.fullPortRegex.r
        (options.vector.instance, options.vector.instanceOffset, options.vector.jdbcPort) match {
            case (Some(instance(_*)), None, None) |
                 (Some(instance(_*)), Some(offset(_*)), None) |
                 (None, None, Some(port(_*))) => success
            case _ => failure("Error is: EITHER instance id (vi) and optional instance offset (vo) OR real JDBC port number (vj) required!")
        }
    } else {
      failure("Errors are: " + missing.map(n => s"${n} missing").mkString(";"))
    }
  }
}
