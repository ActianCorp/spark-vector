package com.actian.spark_vectorh.vector

/**
 * Error codes to return in exceptions from Vector ops
 */
object ErrorCodes {
  /** A column is non null but not loaded */
  val missingNonNullColumn = 1
  /** Too many/few input fields for columns */
  val invalidNumberOfInputs = 2
  /** Reference to a non-existing column */
  val noSuchColumn = 3
  /** No columns are mapped for loading */
  val noColumnsMapped = 4
  /** Target table does not exist */
  val noSuchTable = 5
  /** VWLoad failed */
  val vwloadError = 6
  /** Invalid ssh credentials */
  val sshCredentialsInvalid = 7
  /** Error communicating with lead node */
  val communicationError = 8
  /** Address of HDFS namenode is invalid */
  val nameNodeAddressInvalid = 9
  /** Missing JDBC driver */
  val missingDriver = 10
  /** Ran the vwinfo command and it failed */
  val vwinfoFailed = 11
  /** Error running the pre or post SQL statement(s) */
  val sqlExecutionError = 12
  /** Host for the given name does not exist */
  val noSuchHost = 13
  /** Exception thrown by Vector during SQL execution */
  val sqlException = 14
  /** I/O error in target filesystem */
  val fileSystemError = 15
  /** Mapping from a missing input field */
  val noSuchSourceField = 16

}
