package gallia

// ===========================================================================
package object spark {
  type ClassTag[A]  = scala.reflect.ClassTag[A]

  type SparkContext = org.apache.spark.SparkContext

  type RDD     [A]  = org.apache.spark.rdd.RDD[A]
  type Streamer[A]  = gallia.data.multiple.Streamer[A]

  type Line = aptus.Line

  type RddStreamer[A] = gallia.data.multiple.streamer.RddStreamer[A]
  val  RddStreamer    = gallia.data.multiple.streamer.RddStreamer

  // ===========================================================================
  implicit class StartReadZFluency__(override val conf : gallia.io.in.StartReadZFluency) extends StartReadFluencyRDD
  implicit class HeadZ__            (override val headZ: gallia.HeadZ)                   extends HeadZRdd

  // ===========================================================================
  object logging {
    def setToWarn() { setTo(org.apache.log4j.Level.WARN) }
    def setTo(level: org.apache.log4j.Level) { aptus.spark.SparkLogging.setLoggingTo(level) }
  }
}

// ===========================================================================