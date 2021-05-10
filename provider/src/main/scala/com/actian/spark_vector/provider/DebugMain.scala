/*
 * Copyright 2021 Actian Corporation
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

package com.actian.spark_vector.provider

import com.actian.spark_vector.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import resource.managed

import java.io.File
import java.io.PrintWriter
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel

/** Main for localhost debugging with a single, node-local spark master.
  * Vector credentials are directly written into the spark_info_file which can directly
  * be fed into a Vector instance. There is no need to do iisuspark or start_provider
  * from Vector side.
  */
object DebugMain extends App with Logging {
  import ProviderAuth._
  private val conf = new SparkConf()
    .setAppName("Spark-Vector external tables provider")
    .set("spark.task.maxFailures", "1")
    .set("spark.sql.caseSensitive", "false")
    .setMaster("local")

  logInfo(
    s"Starting Spark-Vector provider with config options: ${conf.getAll.toMap}"
  )

  private var builder = SparkSession.builder.config(conf)
  if (conf.getBoolean("spark.vector.provider.hive", true)) {
    builder = builder.enableHiveSupport()
  }
  private val session = builder.getOrCreate()
  private lazy val handler = new RequestHandler(
    session,
    ProviderAuth(generateUsername, generatePassword)
  )
  sys.addShutdownHook {
    session.close()
    logInfo("Shutting down Spark-Vector provider...")
  }
  for {
    server <- managed(
      ServerSocketChannel.open.bind(new InetSocketAddress(0))
    )
  } {

    val writer = new PrintWriter(
      new File(
        "spark_info_file"
      )
    )

    logInfo(
      s"Spark-Vector provider initialized and starting listening for requests on port ${server.socket.getLocalPort}"
    )

    writer.write(
      s"vector_provider_hostname=${InetAddress.getLocalHost.getHostName()}\n"
    )
    writer.write(s"vector_provider_port=${server.socket.getLocalPort}\n")
    writer.write(s"vector_provider_username=${handler.auth.username}\n")
    writer.write(s"vector_provider_password=${handler.auth.password}\n")
    writer.close()

    while (true) handler.handle(server.accept)
  }
}
