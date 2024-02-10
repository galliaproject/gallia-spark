package aptus
package spark

import org.apache.log4j.{Level, Logger}

// ===========================================================================
object SparkLogging {
  def setLoggingToWarn(): Unit = { setLoggingTo(Level.WARN) }

  // ---------------------------------------------------------------------------
  def setLoggingTo(level: Level): Unit = {
    // TODO: t210122092619 - get rid of "Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties"

    Logger.getLogger("org.apache.spark" ).setLevel(level)
    Logger.getLogger("org.apache.hadoop").setLevel(level) /* for eg: "... INFO FileInputFormat: Total input paths to process : ..." */ } }

// ===========================================================================
