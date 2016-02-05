package com.actian.spark_vectorh.test.tags

import org.scalatest.Tag

/**
 * Test tags used to differentiate test categories.
 */

object IntegrationTest extends Tag("com.actian.dataflow.test.tags.IntegrationTest")

object RandomizedTest extends Tag("com.actian.dataflow.test.tags.RandomizedTest")
