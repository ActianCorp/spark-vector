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
package com.actian.spark_vector

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.scalatest.{ fixture, Outcome }

/**
 * Provides a SparkContext fixture to tests that require one. Creates a new SparkContext for
 * each test and tears it down when the test completes. This allows each test that uses Spark
 * to create a unique event log also as needed for later analysis.
 */
trait SparkContextFixture { this: fixture.Suite =>
  case class FixtureParam(spark: SparkSession) {
    val sc = spark.sparkContext
  }

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
    val spark = SparkSession.builder
                            .appName(test.name)
                            .master(getMaster(test.name))
                            .config(config)
                            .getOrCreate()
    val contextFixture = FixtureParam(spark)

    try {
      // Run the test
      withFixture(test.toNoArgTest(contextFixture))
    } finally spark.stop() // shut down spark session
  }

}

object SparkContextFixture {
  // Useful for test suites where a subset of tests require Spark
  def withSpark(appName: String = "test", master: String = "local[*]")(op: SparkSession => Unit): Unit = {
    val config = new SparkConf(false)

    val spark = SparkSession.builder
                            .appName(appName)
                            .master(master)
                            .config(config)
                            .getOrCreate()

    try {
      op(spark)
    } finally {
      // Shut down Spark context after every test
      spark.stop()
    }
  }
}
