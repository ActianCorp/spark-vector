package com.actian.spark_vectorh.vector

/**
 * Error codes to return in exceptions from Vector ops
 */
object ErrorCodes {

  val missingNonNullColumn = 1      // A column is non null but not loaded
  val invalidNumberOfInputs = 2     // Too many/few input fields for columns
  val noSuchColumn = 3              // Reference to a non-existing column
  val noColumnsMapped = 4           // No columns are mapped for loading
  val noSuchTable = 5               // Target table does not exist
  val vwloadError = 6               // Vwload failed
  val sshCredentialsInvalid = 7     // Invalid ssh credentials
  val communicationError = 8        // Error communicating with lead node
  val nameNodeAddressInvalid = 9    // Address of HDFS namenode is invalid
  val missingDriver = 10            // Missing JDBC driver
  val vwinfoFailed = 11             // Ran the vwinfo command and it failed
  val sqlExecutionError = 12        // Error running the pre or post SQL statement(s)
  val noSuchHost = 13               // Host for the given name does not exist
  val sqlException = 14             // Exception thrown by Vector during SQL execution
  val fileSystemError = 15          // I/O error in target filesystem
  val noSuchSourceField = 16        // Mapping from a missing input field

}
