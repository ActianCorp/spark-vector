package com.actian.spark_vector.external_table

import com.actian.spark_vector.vector.JDBCPort
import com.actian.spark_vector.vector.VectorConnectionProperties
import org.apache.spark.launcher.SparkLauncher
import org.scalatest.FixtureTestSuite
import org.scalatest._

import java.io.BufferedReader
import scala.concurrent.duration._
import scala.io.Source

/** This trait enables integration tests against the spark provider.
  * The spark provider is started as a sub process.
  * Intended use case: Send SQL queries using external table feature
  * via JDBC to Vector/VectorH which itself will use the started spark provider instance.
  * This scenario can be seen as a start of a replacement for current fastload_spark(X) sep tests.
  */
trait ProviderFixture { this: FixtureTestSuite =>

  def master: String

  def providerJar: String = System.getProperty("provider.jar", "")
  def providerSparkInfoFile: String =
    System.getProperty("provider.sparkInfoFile", "")
  def providerSparkHome: String = System.getProperty("provider.sparkHome", "")

  def connectionProps: VectorConnectionProperties = {
    val host = System.getProperty("vector.host", "")
    val instance = System.getProperty("vector.instance", "")
    val jdbcPort = System.getProperty("vector.jdbcPort", "")
    val instanceOffset = System.getProperty(
      "vector.instanceOffset",
      if (jdbcPort.isEmpty) JDBCPort.defaultInstanceOffset else ""
    )
    val database = System.getProperty("vector.database", "")
    val user = System.getProperty("vector.user", "")
    val password = System.getProperty("vector.password", "")

    VectorConnectionProperties(
      host,
      JDBCPort(
        Some(instance).filter(!_.isEmpty),
        Some(instanceOffset).filter(!_.isEmpty),
        Some(jdbcPort).filter(!_.isEmpty)
      ),
      database,
      Some(user).filter(!_.isEmpty),
      Some(password).filter(!_.isEmpty)
    )
  }

  case class FixtureParam(provider: Process)

  override protected def withFixture(test: OneArgTest): Outcome = {
    val launcher =
      new SparkLauncher()
        .addAppArgs(
          "-m",
          master,
          "-p",
          providerSparkInfoFile
        )
        .setMaster(master)
        .setDeployMode("client")
        .setMainClass("com.actian.spark_vector.provider.DebugMain")
        .setSparkHome(
          providerSparkHome
        )
        .setAppResource(
          providerJar
        )

    var provider: Process = null
    try {
      provider = launcher.launch()
      val reader = new BufferedReader(
        Source.fromInputStream(provider.getInputStream()).reader()
      )
      val deadline = 60.seconds.fromNow
      var found = false
      while (!found) {
        if (
          reader
            .readLine()
            .contains(
              s"Spark-Vector provider initialized and starting listening for requests on port"
            )
        ) {
          found = true
        } else {
          if (deadline.isOverdue())
            throw new RuntimeException(
              "Spark-provider could not be started."
            )
        }
      }
      val fixture = FixtureParam(provider)
      withFixture(test.toNoArgTest(fixture))
    } catch {
      case e: Exception => Failed(e.getMessage())
    } finally {
      if (provider != null) {
        provider.destroyForcibly()
      }
    }
  }
}

/** This trait enables local multi-threaded deployment of the spark provider.
  */
trait LocalProviderFixture extends ProviderFixture { this: FixtureTestSuite =>
  override def master: String = "local[*]"
}
