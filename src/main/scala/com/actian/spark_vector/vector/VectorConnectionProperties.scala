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
package com.actian.spark_vector.vector


trait JDBCPort extends Serializable {
    def value: String

    override def toString = value
}

object JDBCPort {
    private class JDBCPortImpl(port: String) extends JDBCPort { override def value = {port} }

    private def apply(instance: String, instanceOffset: Option[String]): JDBCPort = {
        require(instance.matches("[a-zA-Z]+"), "The instance property and cannot be empty or a number")
        val offset: String = {
            instanceOffset match {
                case Some(x) => {
                    require(x.matches("[0-9]+"), "Instance offset cannot be empty and has to be a number!")
                    x}
                case None => "7"
            }
        }
        new JDBCPortImpl(s"${instance}${offset}")
    }

    private def apply(port: String): JDBCPort = {
        val regex = """[a-zA-Z]*[0-9]+""".r
        port match {
            case regex() => new JDBCPortImpl(port)
            case _ => throw new IllegalArgumentException("Port is not valid!")
        }
    }

    def apply(instance: Option[String], instanceOffset: Option[String], port: Option[String]): JDBCPort = {
        require((instance.isDefined && !port.isDefined) || (!instance.isDefined && !instanceOffset.isDefined && port.isDefined),
            "EITHER instance id and optional instance offset OR real port number required!")
        instance match {
            case Some(x) => JDBCPort(x, instanceOffset)
            case None => JDBCPort(port.get)
        }
    }
}

/**
 * Container for Vector connection properties.
 */
case class VectorConnectionProperties(host: String,
    port: JDBCPort,
    database: String,
    user: Option[String] = None,
    password: Option[String] = None) extends Serializable {
  require(host != null && host.length > 0, "The host property is required and cannot be null or empty")
  require(database != null && database.length > 0, "The database property is required and cannot be null or empty")

  def toJdbcUrl: String = s"jdbc:ingres://${host}:${port}/${database}"
}
