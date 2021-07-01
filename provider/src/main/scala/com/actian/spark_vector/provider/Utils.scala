package com.actian.spark_vector.provider

/** Contains arbitrary utility functions.
  */
object Utils {

  /** Given a job part, retrieve its options, if any, or else an empty Map
    *
    * @param part External table request.
    * @return map of options, if any, otherwise an empty map.
    */
  def getOptions(part: JobPart): Map[String, String] = {
    var options = part.options
      .getOrElse(Map.empty[String, String])
      .map({ case (k, v) => k.toLowerCase -> v })
      .filterKeys(k => !part.extraOptions.contains(k.toLowerCase()))

    if (part.format.contains("csv") && !options.contains("header")) {
      options + ("header" -> "true");
    } else {
      options;
    }
  }
}
