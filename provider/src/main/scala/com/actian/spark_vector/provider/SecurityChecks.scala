package com.actian.spark_vector.provider

/** Contains arbitrary security checks.
  */
object SecurityChecks {

  val enabled: Boolean = true

  /** Checks whether either the JDBC connection url or
    * the OPTIONS section of an external table JDBC request
    * contains non-empty user and password.
    *
    * @param jobPart External table JDBC request.
    * @return (true, "") if user and password were found, (false, "ErrorMessage") otherwise.
    */
  private def checkUserPasswordProvidedJDBC(
      jobPart: JobPart
  ): (Boolean, String) = {
    val jdbcRegex =
      ".*((UID|user)=.+;(PWD|password)=.+|(PWD|password)=.+;(UID|user)=.+)".r
    val options = Utils.getOptions(jobPart)

    // Unlike JDBC url, we don't have UID and PWD here.
    val specifiedInOptions = options
      .filter(kv =>
        kv match {
          case ("user", value) => !value.isEmpty
          case _               => false
        }
      )
      .size == 1 && options
      .filter(kv =>
        kv match {
          case ("password", value) => !value.isEmpty
          case _                   => false
        }
      )
      .size == 1

    val specifiedInURL =
      jdbcRegex.pattern.matcher(options.getOrElse("url", "")).matches()

    (specifiedInOptions, specifiedInURL) match {
      case (true, true) =>
        (
          false,
          "User and password are EITHER allowed in JDBC url OR under OPTIONS!"
        )
      case (false, false) => (false, "No user name and/or password provided!")
      case _              => (true, "")
    }
  }

  /** Checks, whether the given external table request contains
    * necessary user credentials. In case they are missing, a
    * SecurityException is thrown.
    *
    * @param jobPart External table request.
    */
  def checkUserPasswordProvided(jobPart: JobPart): Unit = {
    if (!enabled) return
    jobPart.format match {
      case Some(value) =>
        value.toLowerCase match {
          case "jdbc" =>
            checkUserPasswordProvidedJDBC(jobPart: JobPart) match {
              case (false, m) => throw new SecurityException(m)
              case _          =>
            }
          case _ =>
        }
      case _ =>
    }
  }
}
