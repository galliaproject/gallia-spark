package gallia

import aptus.spark._

// ===========================================================================
package object spark {
  type Line = aptus.Line

  // ---------------------------------------------------------------------------
  type SparkContext = org.apache.spark.SparkContext
  type RDD[A]       = org.apache.spark.rdd.RDD[A]

  // ---------------------------------------------------------------------------
  type Streamer[A] = gallia.streamer.Streamer[A]

  type RddStreamer[A] = gallia.streamer.RddStreamer[A]
  val  RddStreamer    = gallia.streamer.RddStreamer

  // ---------------------------------------------------------------------------
  type ActionIZ01 = actions.boilerplate.ActionBoilerplate.ActionIZ01
  type ActionZZ01 = actions.boilerplate.ActionBoilerplate.ActionZZ01
  type ActionZO1N = actions.boilerplate.ActionBoilerplate.ActionZO1N

  // ===========================================================================
  private[gallia] implicit class GalliaSparkAnything_[A](value: A) {
    /** so as to not conflict with Spark RDD's own pipe method */
    private[gallia] def pype[B](f: A => B): B = f(value) }

  // ===========================================================================
  def galliaSparkContext(name: AppName = DefaultAppName): SparkContext = SparkDriver.context(name, managed = false)

  // ===========================================================================
  def unregisterSparkContext() = { gallia.Hacks.sparkRddHack.clear() }

  // ---------------------------------------------------------------------------
  def registerSparkContext(sc: SparkContext) = { // 220721104754
    if (!gallia.Hacks.sparkRddHack.isSet()) {
      gallia.Hacks.sparkRddHack.setValue(
        SparkRddUtils.sparkRddHack(sc)) } }

  // ---------------------------------------------------------------------------
  val numPartitionsHack: ThreadLocal[Option[Int]] = ThreadLocal.withInitial(() => None)

    private[gallia] def numPartitions(sc: SparkContext): Int = numPartitionsHack.get().getOrElse(sc.defaultMinPartitions)

  // ===========================================================================
  implicit class SparkStartReadZFluency__(override val conf : io.in.StartReadZFluency) extends StartReadFluencyRDD
  implicit class SparkHeadZ_             (override val headZ: HeadZ)                   extends gallia.heads.HeadZRdd
  implicit class SparkBObjs_             (bobjs:              BObjs)                   extends gallia.heads.HeadZRdd { override val headZ: HeadZ = bobjs }
  implicit class SparkAObjs_             (aobjs:              AObjs)                   extends gallia.heads.HeadZRdd { override val headZ: HeadZ = aobjs }

  // ---------------------------------------------------------------------------
  implicit class SparkContext_(sc: SparkContext) {
    def rdd(schema: Cls, rdd: RDD[Obj]): HeadS = SparkRddIn.rdd(sc, schema, rdd)

    // ---------------------------------------------------------------------------
    def lines(path: String): HeadS = SparkRddIn.lines(sc, path)

    // ---------------------------------------------------------------------------
    def jsonLines(schema: Cls)            (path: String): HeadS = SparkRddIn.jsonLines(sc, schema, path)
    def jsonLines(field1: Fld, more: Fld*)(path: String): HeadS = SparkRddIn.jsonLines(sc, cls((field1 +: more).toList), path)

    // ---------------------------------------------------------------------------
    def csvWithHeader(path: String)(key1: KeyW, more: KeyW*): HeadS = SparkRddIn.csvWithHeader(sc, path)(key1, more:_*)
    def tsvWithHeader(path: String)(key1: KeyW, more: KeyW*): HeadS = SparkRddIn.tsvWithHeader(sc, path)(key1, more:_*)
  } 

}

// ===========================================================================
