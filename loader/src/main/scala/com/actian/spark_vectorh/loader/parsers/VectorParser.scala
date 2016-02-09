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

sealed class VectorArgDescription(val longName: String, val shortName: String, val description: String)

sealed case class VectorArgOption[T: Read, O](
  lName: String,
  sName: String,
  desc: String,
  extractor: UserOptions => O,
  injector: (T, UserOptions) => UserOptions,
  mandatory: Boolean) extends VectorArgDescription(lName, sName, desc) {
  def asOpt(parser: scopt.OptionParser[UserOptions]): OptionDef[T, UserOptions] =
    parser.opt[T](longName) abbr (shortName) action (injector) text (description)
}

object VectorArgs {
  import UserOptions._
  implicit object ReadChar extends Read[Char] {
    override def arity: Int = 1

    override def reads: (String) => Char = _.head
  }

  val load = new VectorArgDescription("load", "lh", "Read a file and load into Vector")
  val csvLoad = new VectorArgDescription("csv", "csv", "Load a csv file")
  val parquetLoad = new VectorArgDescription("parquet", "parquet", "Load a parquet file")

  val vectorHost = VectorArgOption[String, String](
    "vectorHost", "vh", "Vector host URL", _.vector.host, updateVector((o, v) => o.copy(host = v)), true)
  val vectorInstance = VectorArgOption[String, String](
    "vectorInstance", "vi", "Vector instance", _.vector.instance, updateVector((o, v) => o.copy(instance = v)), true)
  val vectorDatabase = VectorArgOption[String, String](
    "vectorDatabase", "vd", "Vector database", _.vector.database, updateVector((o, v) => o.copy(database = v)), true)
  val vectorUser = VectorArgOption[String, Option[String]](
    "vectorUser", "vu", "Vector user", _.vector.user, updateVector((o, v) => o.copy(user = Some(v))), false)
  val vectorPassword = VectorArgOption[String, Option[String]](
    "vectorPass", "vp", "Vector password", _.vector.password, updateVector((o, v) => o.copy(password = Some(v))), false)
  val vectorTargetTable = VectorArgOption[String, String](
    "vectorTargetTable", "tt", "Vector target table", _.vector.targetTable, updateVector((o, v) => o.copy(targetTable = v)), true)
  val vectorCreateTable = VectorArgOption[Boolean, Option[Boolean]](
    "createTable", "ct", "create Vector target table", uo => uo.vector.createTable, updateVector((o, v) => o.copy(createTable = Some(v))), false)

  val vectorArgs = Seq(vectorHost, vectorInstance, vectorDatabase, vectorUser, vectorPassword, vectorTargetTable, vectorCreateTable)

  val inputFile = VectorArgOption[String, String](
    "sourceFile", "sf", "Source file", _.general.sourceFile, updateGeneral((o, v) => o.copy(sourceFile = v.replace('\\', '/'))), true)
  val colsToLoad = VectorArgOption[String, Option[Seq[_]]](
    "cols", "cols", "comma separated string containing only column names to load", _.general.colsToLoad, updateGeneral((o, v) => o.copy(colsToLoad = Some(v.split(",")))), false)

  val generalArgs = Seq(inputFile, colsToLoad)

  val hRow = VectorArgOption[Boolean, Option[Boolean]](
    "skipHeader", "sh", "skip header row", _.csv.headerRow, updateCSV((o, v) => o.copy(headerRow = Option(v))), false)
  val encoding = VectorArgOption[String, Option[String]](
    "encoding", "en", "CSV text encoding", _.csv.encoding, updateCSV((o, v) => o.copy(encoding = Option(v))), false)
  val nullPattern = VectorArgOption[String, Option[String]](
    "nullPattern", "np", "CSV null pattern", _.csv.nullPattern, updateCSV((o, v) => o.copy(nullPattern = Option(v))), false)
  val separatorChar = VectorArgOption[Char, Option[Char]](
    "separatorChar", "sc", "CSV field separator char", _.csv.separatorChar, updateCSV((o, v) => o.copy(separatorChar = Option(v))), false)
  val quoteChar = VectorArgOption[Char, Option[Char]](
    "quoteChar", "qc", "CSV quote char", _.csv.quoteChar, updateCSV((o, v) => o.copy(quoteChar = Option(v))), false)
  val escapeChar = VectorArgOption[Char, Option[Char]](
    "escapeChar", "ec", "CSV escape char", _.csv.escapeChar, updateCSV((o, v) => o.copy(escapeChar = Option(v))), false)
  val header = VectorArgOption[String, Option[Seq[_]]](
    "header", "h", "comma separated string with CSV column names and datatypes", _.csv.header, updateCSV((o, v) => o.copy(header = Some(v.split(",")))), false)
  val parserLib = VectorArgOption[String, Option[String]](
    "parserLib", "pl", "CSV parser library: either default or univocity", _.csv.nullPattern, updateCSV((o, v) => o.copy(parserLib = Option(v))), false)

  val csvArgs = Seq(hRow, encoding, nullPattern, separatorChar, quoteChar, escapeChar, header, colsToLoad, parserLib)
  val parquetArgs = Seq.empty[VectorArgOption[_, _]]

  val csvOptions: Seq[VectorArgOption[_, _]] = generalArgs ++ vectorArgs ++ csvArgs
  val parquetOptions: Seq[VectorArgOption[_, _]] = generalArgs ++ vectorArgs ++ parquetArgs

  val modeToOptions = Map(csvLoad.longName -> csvOptions, parquetLoad.longName -> parquetArgs)
}

object VectorParser extends scopt.OptionParser[UserOptions]("Spark Vector load tool") {
  import VectorArgs._

  head("Spark Vector load tool", "1.0.0")
  note("Spark Vector load")
  help("help").text("This tool can be used to load CSV/Parquet files through Spark to Vector")
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
        .children(parquetOptions.map(_.asOpt(this)): _*))

  checkConfig { options =>
    if (options.mode.isEmpty || options.mode == load.longName) {
      failure(s"Invalid command. Available commands are ${modeToOptions.keys.map(mode => s"load $mode").mkString(",")}")
    } else {
      validateOptions(options)
    }
  }

  private def validateOptions(options: UserOptions): Either[String, Unit] = {
    // TODO: Risky assumption. Fix when the first non-string required configuration option occurs.
    val required = modeToOptions(options.mode).filter(_.mandatory).map(_.asInstanceOf[VectorArgOption[_, String]])
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
