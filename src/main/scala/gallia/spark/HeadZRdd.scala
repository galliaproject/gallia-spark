package gallia.spark

import gallia._

// ===========================================================================
trait HeadZRdd { val headZ: HeadZ
  def toListCounterpart: HeadZ = ???
  def toRDDCounterpart : HeadZ = ???

  // ---------------------------------------------------------------------------
  def rdd(f: RDD[Obj] => RDD[Obj]): HeadZ =
    headZ._modifyUnderlyingStreamer(_ match {
      case rddStreamer: RddStreamer[Obj] => rddStreamer._modifyUnderlyingRdd(f)
      case    streamer                   => gallia.runtimeError("TODO:201106123320:not RDD-based") /* TODO: catch earlier? */ })

   // ---------------------------------------------------------------------------
   def toRDD: RDD[Obj] = ??? // also see t201216101136
}

// ===========================================================================
