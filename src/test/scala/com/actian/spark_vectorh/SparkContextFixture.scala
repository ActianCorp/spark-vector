package com.actian.spark_vectorh

import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ fixture, Outcome }

/**
 * Provides a SparkContext fixture to tests that require one. Creates a new SparkContext for
 * each test and tears it down when the test completes. This allows each test that uses Spark
 * to create a unique event log also as needed for later analysis.
 */
trait SparkContextFixture { this: fixture.Suite =>
  case class FixtureParam(sc: SparkContext)

  // Give test suite opportunity to set up config
  def setupSparkConf(testName: String, sparkConf: SparkConf) {}

  // And override master setting
  def getMaster(testName: String): String = {
    "local[*]"
  }

  def withFixture(test: OneArgTest): Outcome = {
    val config = new SparkConf(false)
    setupSparkConf(test.name, config)

    // Create context and fixture
    val sc: SparkContext = new SparkContext(getMaster(test.name), test.name, config)
    val contextFixture = FixtureParam(sc)

    try {
      // Run the test
      withFixture(test.toNoArgTest(contextFixture))
    } finally sc.stop() // shut down spark context
  }

}

object SparkContextFixture {
  // Useful for test suites where a subset of tests require Spark
  def withSpark(appName: String = "test", master: String = "local[*]")(op: SparkContext => Unit): Unit = {
    val config = new SparkConf(false)

    val sc = new SparkContext(master, appName, config)

    try {
      op(sc)
    } finally {
      // Shut down Spark context after every test
      sc.stop()
    }
  }
}
