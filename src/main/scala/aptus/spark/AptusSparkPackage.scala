package aptus

// ===========================================================================
package object spark {
  type AppName   = String
  type UrlString = String

  // ===========================================================================
  val DefaultSparkHome = "/tmp/spark"
  val DefaultAppName   = "default"
  val DefaultMaster    = "local[*, 3]" // TODO: pass individually rather; TODO: default to # of cores -1?
  val DefaultPort      = 7077

  // ===========================================================================
  private[spark] val _cache = aptus.spark.SparkContextCache
}

// ===========================================================================