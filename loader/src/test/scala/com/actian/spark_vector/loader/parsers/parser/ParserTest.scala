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

import com.actian.spark_vector.loader.options.UserOptions
import org.scalatest.prop.PropertyChecks
import org.scalatest.{ Matchers, Inspectors, FunSuite }

object ParserTest {
  type ArgMap = Map[ArgOption[_, _], Any]
  type KeyMapper = ArgDescription => String

  def shortKey(opt: ArgDescription): String = s"-${opt.shortName}"
  def longKey(opt: ArgDescription): String = s"--${opt.longName}"

  object ShortKeyMapper extends KeyMapper {
    override def apply(arg: ArgDescription): String = shortKey(arg)
    override def toString(): String = getClass.getSimpleName
  }

  object LongKeyMapper extends KeyMapper {
    override def apply(arg: ArgDescription): String = longKey(arg)
    override def toString(): String = getClass.getSimpleName
  }
}

class ParserTest extends FunSuite with Matchers with PropertyChecks {

  import ParserTest._
  import ArgOption._
  import scala.language.existentials
  import Args._

  val requiredValues: ArgMap = Map(
    vectorHost -> "vector.test",
    vectorInstance -> "VH",
    vectorDatabase -> "testdb",
    vectorTargetTable -> "testtbl",
    inputFile -> "/tmp/test.csv")

  val optionalValues: ArgMap = Map(
    hRow -> true,
    inferSchema -> false,
    encode -> "UTF-16",
    separatorChar -> '|',
    quoteChar -> '\'',
    escapeChar -> '~',
    commentChar -> '\\',
    ignoreLeading -> true,
    ignoreTrailing -> true,
    nullValue -> "N/A",
    nanValue  -> "nan",
    positiveInf -> "+I",
    negativeInf -> "-I",
    dateFormat -> "dd/MM/yyyy",
    timestampFormat -> "dd/MM/yyyy/HH:mm:ss",
    parseMode -> "failfast",
    vectorUser -> "johndoe",
    vectorPassword -> "p@55")

  val csvValues: ArgMap =
    requiredValues ++ optionalValues

  test("metadata") {
    val parser = Parser
    assert(parser.header.contains("Spark Vector load tool"))
    assert(parser.programName.startsWith("spark-submit --class com.actian.spark_vector.loader.Main"))
  }

  test("parse full") {
    val keyMapperCandidates = Seq[KeyMapper](LongKeyMapper, ShortKeyMapper)
    val dataCandidates = Map(
      "csv" -> Seq[ArgMap](csvValues, requiredValues),
      "parquet" -> Seq[ArgMap](requiredValues))

    val candidates =
      for {
        keyMapper <- keyMapperCandidates
        loadType <- dataCandidates.keys
        data <- dataCandidates(loadType)
      } yield (data, keyMapper, loadType)
    val table = Table(
      ("data", "keyMapper", "load type"),
      candidates: _*)
    forAll(table)(assertParseFull)
  }

  test("parse required missing") {
    val parser = Parser
    val table = Table("arg", requiredValues.keys.toSeq: _*)
    forAll(table)(arg => {
      val input = inputFromArgs(requiredValues - arg, LongKeyMapper, "csv")
      parser.parse(input, UserOptions()) shouldBe 'empty
    })
  }

  test("windows file path") {
    val parser = Parser
    val args = Seq(
      load.longName,
      csvLoad.longName,
      longKey(vectorHost), "vector.test",
      longKey(vectorInstance), "VH",
      longKey(vectorDatabase), "testdb",
      longKey(vectorUser), "johndoe",
      longKey(vectorPassword), "p@55",
      longKey(vectorTargetTable), "testtbl",
      longKey(inputFile), """c:\tmp\test.csv""")
    parser.parse(args, UserOptions()) match {
      case None => fail("Could not parse args")
      case Some(options) => options.general.sourceFile should be("c:/tmp/test.csv")
    }
  }

  test("JDBC port specification") {
    val parser = Parser
    val base = Seq(
      load.longName,
      csvLoad.longName,
      longKey(vectorHost), "vector.test",
      longKey(vectorDatabase), "testdb",
      longKey(vectorUser), "johndoe",
      longKey(vectorPassword), "p@55",
      longKey(vectorTargetTable), "testtbl",
      longKey(inputFile), """c:\tmp\test.csv""")
    val portValuesSets = Seq(None, Some(""), Some("VW"), Some("VW7"), Some("9999"))
    val keyMapperCandidates = Seq(LongKeyMapper, ShortKeyMapper)

    val testData = for {
        keymapper <- keyMapperCandidates
        portValueInstance <- portValuesSets
        portValueOffset <- portValuesSets
        portValueJDBC <- portValuesSets
    } yield (keymapper, portValueInstance, portValueOffset, portValueJDBC)

    val table = Table(
      ("keymapper", "portValueInstance", "portValueOffset", "portValueJDBC"),
      testData: _*)

    forAll(table)( (keymapper, portValueInstance, portValueOffset, portValueJDBC) => {
        var testsequence: Seq[String] = Seq();
        if(portValueInstance.isDefined) testsequence ++= Seq(keymapper(vectorInstance), portValueInstance.get)
        if(portValueOffset.isDefined) testsequence ++= Seq(keymapper(vectorInstOffset), portValueOffset.get)
        if(portValueJDBC.isDefined) testsequence ++= Seq(keymapper(vectorJDBCPort), portValueJDBC.get)
        testsequence = base ++ testsequence

        (portValueInstance, portValueOffset, portValueJDBC) match {
            case (None, None, None) | (_, Some(_), Some(_)) | (Some(_), _, Some(_)) |
                 (Some(""), _, _) | (_, Some(""), _) | (_, _, Some("")) |
                 (Some("VW7"), _, _) | (_, Some("VW7"), _) |
                 (_, Some("VW"), _) | (_, _, Some("VW")) |
                 (Some("9999"), _, _) | (None, Some("9999"), _) => parser.parse(testsequence, UserOptions()) shouldBe None
            case _ => parser.parse(testsequence, UserOptions()) shouldBe defined
        }
    })
  }

  private def valueStr(value: Any): String =
    value match {
      case Some(v) => v.toString
      case v => v.toString
    }

  private def inputFromArgs(data: ArgMap, keyMap: KeyMapper, loadType: String): Seq[String] =
    "load" +: loadType +:
      data.toSeq.map {
        case (arg, value) => Seq(keyMap(arg), valueStr(value))
      }.flatten

  private def assertParseFull(data: ArgMap, keyMap: KeyMapper, loadType: String): Unit = {
    val input = inputFromArgs(data, keyMap, loadType)
    val parser = Parser
    parser.parse(input, UserOptions()) match {
      case Some(opts) =>
        opts.mode should be(loadType)
        Inspectors.forAll(data) {
          case (arg, value) => arg.extractor(opts) should be(if (arg.mandatory) value else Option(value))
        }
      case None => fail("Parse failure.")
    }
  }
}
