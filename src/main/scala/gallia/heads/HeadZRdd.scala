package gallia
package heads

import gallia.spark._

// ===========================================================================
trait HeadZRdd { val headZ: HeadZ
  def toRddBased: HeadZ = headZ ::+ ToRddBased

    case object ToRddBased extends IdentityVM1 with ActionZZd { def atomzz = _ToRddBased }

      case object _ToRddBased extends AtomZZ { def naive(z: Objs) = z._toRddBased }

  // ===========================================================================
  def rdd(f: RDD[Obj] => RDD[Obj]): HeadZ =
    headZ._modifyUnderlyingStreamer(_ match {
      case rddStreamer: RddStreamer[Obj] => rddStreamer._alter(f)
      case _                             => gallia.dataError("TODO:201106123320:not RDD-based") /* TODO: catch earlier? */ })

  // =========================================================================== 
  @annotation.nowarn def writeRDD(path: String): Unit = { // TODO: align with write abstraction
    headZ
      .zo(SparkRddOut.RddOutputZ(path))
      .end ()
      .runz().either match {
        case Left (errors) => throw errors.metaErrorOpt.get
        case Right(_)      => () } } }

// ===========================================================================
