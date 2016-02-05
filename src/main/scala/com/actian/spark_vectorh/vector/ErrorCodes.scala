package com.actian.spark_vectorh.vector

/**
 * Error codes to return in exceptions from Vector ops
 */
object ErrorCodes {
  /** A column is non null but not loaded */
  val MissingNonNullColumn = 1
  /** Too many/few input fields for columns */
  val InvalidNumberOfInputs = 2
  /** Reference to a non-existing column */
  val NoSuchColumn = 3
  /** No columns are mapped for loading */
  val NoColumnsMapped = 4
  /** Target table does not exist */
  val NoSuchTable = 5
  /** VWLoad failed */
  val VwloadError = 6
  /** Invalid ssh credentials */
  val SshCredentialsInvalid = 7
  /** Error communicating with lead node */
  val CommunicationError = 8
  /** Address of HDFS namenode is invalid */
  val NameNodeAddressInvalid = 9
  /** Missing JDBC driver */
  val MissingDriver = 10
  /** Ran the vwinfo command and it failed */
  val VwinfoFailed = 11
  /** Error running the pre or post SQL statement(s) */
  val SqlExecutionError = 12
  /** Host for the given name does not exist */
  val NoSuchHost = 13
  /** Exception thrown by Vector during SQL execution */
  val SqlException = 14
  /** I/O error in target filesystem */
  val FileSystemError = 15
  /** Mapping from a missing input field */
  val NoSuchSourceField = 16
  /** Authentication error */
  val AuthError = 17
}
