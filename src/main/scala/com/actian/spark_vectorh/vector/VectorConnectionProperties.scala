package com.actian.spark_vectorh.vector

/**
 * Container for VectorH connection properties.
 */
case class VectorConnectionProperties(host: String,
  instance: String,
  database: String,
  user: Option[String] = None,
  password: Option[String] = None) extends Serializable {

  require(host != null && host.length > 0, "The host property is required and cannot be null or empty")
  require(instance != null && instance.length > 0, "The instance property is required and cannot be null or empty")
  require(database != null && database.length > 0, "The database property is required and cannot be null or empty")

  def toJdbcUrl: String = {
    s"jdbc:ingres://${host}:${instance}7/${database}"
  }
}
