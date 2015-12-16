package com.actian.spark_vectorh.vector

/**
 * Exception interacting with Vector.
 */
case class VectorException(val errorCode: Int,
    message:String,
    cause: Throwable = null) extends RuntimeException(message, cause)
