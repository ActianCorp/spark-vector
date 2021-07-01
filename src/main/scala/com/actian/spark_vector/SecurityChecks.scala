package com.actian.spark_vector

import com.actian.spark_vector.vector.VectorConnectionProperties

/** Contains arbitrary security checks.
  */
object SecurityChecks {

  val enabled: Boolean = true

  /** Checks, whether the given JDCB connection properties contain
    * necessary user credentials. In case they are missing, a
    * SecurityException is thrown.
    *
    * @param conProps JDCB connection properties
    */
  def checkUserPasswordProvided(conProps: VectorConnectionProperties): Unit = {
    if (!enabled) return
    conProps.user match {
      case Some(value) if !value.isEmpty =>
      case _ =>
        throw new SecurityException(
          "No user name provided!"
        )
    }
    conProps.password match {
      case Some(value) if !value.isEmpty =>
      case _ =>
        throw new SecurityException(
          "No password provided!"
        )
    }
  }
}
