package com.actian.spark_vector.util

/**
 * Utility to provide various methods.
 * 
 * Note: This is the current solution to the backwards incompatible move in Spark 2.* to make the Utils object
 * used by the Logging trait private. Indeed, most of this has been copied from there:
 * 
 * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
 */
private[spark_vector] object Utils extends Logging {
  
  /**
   * Get the Classloader which loaded Spark.
   */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader
  
  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)
    
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
  }

  def setLogLevel(l: org.apache.log4j.Level) {
    val logger = org.apache.log4j.Logger.getLogger("com.bloomberg.sparkflow")
    logInfo("Changing spark-flow's log level from " + logger.getLevel + " to " + l)

    logger.setLevel(l)
  }
  
}