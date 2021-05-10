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

import akka.actor.ActorSystem
import com.actian.spark_vector.util.Logging
import com.actian.spark_vector.BuildInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import resource.managed
import scopt.OParser
import skuber._
import skuber.json.format._

import java.io.File
import java.io.PrintWriter
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel
import java.security.InvalidParameterException
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/** Main which is intended to be used only in a Kubernetes cluster environment.
  * Vector credentials are directly written into a secret which should be volume-mounted into the Vector master pod.
  * Name and namespace of the secret can be configured via commandline arguments.
  * The secret contains key spark_info_file which maps to the string containing former content of the traditional spark_info_file.
  * If the secret is volume-mounted into the Vector master pod, the key spark_info_file appears as a file which can be symlinked into
  * the required location.
  */
object SparkProviderKubernetes extends App with Logging {
  import ProviderAuth._

  /** Configuration parameters which can be passed in
    * as arguments to [[com.actian.spark_vector.provider.SparkProviderKubernetes]].
    *
    * @param port Listening port of the SparkProviderKubernetes.
    * @param secretName Name of the secret created in Kubernetes.
    * @param secretNamespace Namespace where the secret is created in.
    */
  case class CommandLineOptions(
      port: Int = 0,
      secretName: String = "spark-provider-secret",
      secretNamespace: String = "default"
  )

  val tmpbuilder = OParser.builder[CommandLineOptions]
  val parser = {
    import tmpbuilder._
    OParser.sequence(
      programName(
        s"""spark-submit --class com.actian.spark_vector.provider.SparkProviderKubernetes <spark_vector_provider-assembly-${BuildInfo.version}.jar>"""
      ),
      head(
        s"""Spark-Vector external tables provider, version: ${BuildInfo.version}"""
      ),
      opt[Int]('p', "port")
        .optional()
        .validate(x =>
          if (x > 65535 || x < 0) failure("Port has to be between 0 and 65535.")
          else success
        )
        .action((x, c) => c.copy(port = x))
        .text("Optional port number (default: 0 (random port))."),
      opt[String]('s', "secret")
        .optional()
        .action((x, c) => c.copy(secretName = x))
        .text("Optional secret name (default: spark-provider-secret)."),
      opt[String]('n', "namespace")
        .optional()
        .action((x, c) => c.copy(secretNamespace = x))
        .text("Optional secret namespace (default: default)."),
      help("help").text("Print this usage text!")
    )
  }

  private val cmdOptions: CommandLineOptions =
    OParser.parse(parser, args, CommandLineOptions()) match {
      case None =>
        throw new InvalidParameterException(
          "Wrong usage: Please call with --help to see all options."
        )
      case Some(config) => config
    }

  private val conf = new SparkConf()
    .setAppName("Spark-Vector external tables provider")
    .set("spark.task.maxFailures", "1")
    .set("spark.sql.caseSensitive", "false")

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
      ServerSocketChannel.open.bind(new InetSocketAddress(cmdOptions.port))
    )
  } {

    val secret = Secret(
      metadata = ObjectMeta(
        name = cmdOptions.secretName,
        namespace = cmdOptions.secretNamespace
      ),
      data = Map(
        "spark_info_file" -> s"""vector_provider_hostname=${InetAddress.getLocalHost
          .getHostAddress()}\n
                              |vector_provider_port=${server.socket.getLocalPort}\n
                              |vector_provider_username=${handler.auth.username}\n
                              |vector_provider_password=${handler.auth.password}\n""".stripMargin
          .getBytes()
      )
    )

    // Create the secret in the Kubernetes cluster.
    implicit val system = ActorSystem()
    implicit val dispatcher = system.dispatcher
    val k8s = k8sInit

    val lookup = k8s
      .usingNamespace(cmdOptions.secretNamespace)
      .get[Secret](cmdOptions.secretName)
    val result = lookup
      .map(_ => k8s.update(secret))
      .recover({ case _ => k8s.create(secret) })
    result.onComplete(f =>
      f match {
        case Success(_) =>
        case Failure(e) => throw (e)
      }
    )

    logInfo(
      s"Spark-Vector provider initialized and starting listening for requests on port ${server.socket.getLocalPort}"
    )

    while (true) handler.handle(server.accept)
  }
}
