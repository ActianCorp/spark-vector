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
package com.actian.spark_vector.provider

import java.nio.channels.ServerSocketChannel

import org.apache.spark.{ Logging, SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext

import resource.managed
import java.net.InetAddress

object Main extends App with Logging {
  import ProviderAuth._

  private val conf = new SparkConf()
    .setAppName("Spark-Vector external tables provider")
    .set("spark.task.maxFailures", "1")
  private val sc = new SparkContext(conf)
  private val sqlContext = new HiveContext(sc)

  private lazy val handler = new RequestHandler(sqlContext, ProviderAuth(generateUsername, generatePassword))

  for {
    server <- managed(ServerSocketChannel.open.bind(null))
  } {
    logInfo(s"Spark-Vector provider initialized and starting listening for requests on port ${server.socket.getLocalPort}")
    println("vector_provider_hostname=" + InetAddress.getLocalHost.getHostName)
    println("vector_provider_port=" + server.socket.getLocalPort)
    println("vector_provider_username=" + handler.auth.username)
    println("vector_provider_password=" + handler.auth.password)
    while (true)
      handler.handle(server.accept)
  }
}
