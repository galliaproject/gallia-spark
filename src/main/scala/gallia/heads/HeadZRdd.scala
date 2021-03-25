package gallia.heads

import gallia.spark._
import gallia.atoms.Obj

// ===========================================================================
trait HeadZRdd { val headZ: HeadZ
  def toListCounterpart: HeadZ = ???
  def toRDDCounterpart : HeadZ = ??? // also see t201216101136 (trying Dataset[Obj])

  // ===========================================================================
  def rdd(f: RDD[Obj] => RDD[Obj]): HeadZ =
    headZ._modifyUnderlyingStreamer(_ match {
      case rddStreamer: RddStreamer[Obj] => rddStreamer._modifyUnderlyingRdd(f)
      case    streamer                   => gallia.runtimeError("TODO:201106123320:not RDD-based") /* TODO: catch earlier? */ })

  // =========================================================================== 
  @annotation.nowarn def writeRDD(path: String) { // TODO: align with write abstraction
    headZ
      .zo(out.RddOutputZ(path)).end.runz().either match {
        case Left (errors)  => throw errors.metaErrorOpt.get
        case Right(success) => () } }
   
}

// ===========================================================================
