package gallia.data.multiple.streamer

import aptus.Anything_

import gallia._
import gallia.spark._

// ===========================================================================
class RddStreamer[A: ClassTag /* t210322130619 - generalize to Streamer + as WTT */](val rdd: RDD[A]) extends Streamer[A] {
  val tipe = StreamerType.RDDBased
  private def sc = this.rdd.sparkContext

  // ---------------------------------------------------------------------------
  private         def _rewrap(x: RDD[A]) = RddStreamer.from(x)
  private[gallia] def _modifyUnderlyingRdd(f: RDD[A] => RDD[A]): Streamer[A] = RddStreamer.from(f(rdd))

  // ---------------------------------------------------------------------------
  def egal(that: Streamer[A]): Boolean = ??? // TODO

  def iteratorAndCloseable: (Iterator[A], java.io.Closeable) = (iterator, ??? /* see t210115104555 */)

  def iterator: Iterator[A] = rdd.toLocalIterator // TODO: ok?
  def toView  = ???//: ViewRepr[A] = iterator.toSeq.view - TODO: t210315114618 - causes odd compilation issues with gallia-spark, to investigate
  def toList  : List    [A] = rdd.collect().toList

  def reduce(op: (A, A) => A): A = rdd.reduce(op)

  def     map[B : ClassTag](f: A =>      B ): Streamer[B] = rdd.    map(f).thn(RddStreamer.from)
  def flatMap[B : ClassTag](f: A => Coll[B]): Streamer[B] = rdd.flatMap(f(_).asInstanceOf[SparkColl[B] /*FIXME:t210122102109*/]).thn(RddStreamer.from)

  def filter(p: A => Boolean): Streamer[A] = rdd.filter(p).thn(RddStreamer.from)

  @gallia.NumberAbstraction def size: Int = rdd.count().toInt//FIXME: t210122094438

  def  isEmpty: Boolean = rdd.isEmpty

  // ---------------------------------------------------------------------------
  // TODO: t210122094456 - also sample
  // FIXME: t210312092358 - if n is bigger than partition size (both take/drop); maybe offer "takeFew"/"dropFew" with a set max?
  def take(n: Int): Streamer[A] = rdd.mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.take(n) else itr }.thn(_rewrap)      
  def drop(n: Int): Streamer[A] = rdd.mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.drop(n) else itr }.thn(_rewrap)  
  // TODO: takeLast/dropLast(n): use rdd.getNumPartitions()

  // ===========================================================================
  def distinct: Streamer[A] = _modifyUnderlyingRdd(_.distinct)
  // TODO: ensure distinct - t201126163157 - maybe sort followed by rdd.mapPartitions(_.sliding(size, step), preservesPartitioning)?

  // ===========================================================================
  def sortBy[K](ctag: ClassTag[K], ord: Ordering[K])(f: A => K): Streamer[A] = {
    implicit val y = ctag
    implicit val x = ord

    _modifyUnderlyingRdd(_.sortBy(f)) //TODO: t210122094521 - allow setting numPartitions
  }

  // ===========================================================================
  def groupByKey[K: ClassTag, V: ClassTag](implicit ev: A <:< (K, V)): Streamer[(K, List[V])] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .rdd
      .groupByKey
      .mapValues(_.toList)
      .thn(RddStreamer.from)

  // ===========================================================================
  def union[B >: A : ClassTag](that: Streamer[B]): Streamer[B] =
    RddStreamer.from(
      this.asInstanceOf[RddStreamer[B]].rdd ++
      that.thn(this.asRDDBased)        .rdd)

  // ===========================================================================
  def join[K: ClassTag, V: ClassTag](joinType: JoinType, combiner: (V, V) => V)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[V] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .thn { dis =>
        that.tipe match {
          case StreamerType.ViewBased => // TODO: t210322111234 - [res] - determine if using hash join is implicit (if Seq) or explicit (via conf), or a combination
            RddStreamerUtils
              ._hashJoin(joinType)(
                left  = dis.rdd,
                right = that.thn(dis.asRDDBased).rdd) // TODO: t210322110948 - wasteful: to broadcast directly?  
          case _ =>
            RddStreamerUtils
              ._join(joinType)(
                left  = dis.rdd,
                right = that.thn(dis.asRDDBased).rdd) } }
      .flatMap(RddStreamerUtils.postJoinCombining(combiner))
      .thn(RddStreamer.from)

  // ---------------------------------------------------------------------------
  def coGroup[K: ClassTag, V: ClassTag](joinType: JoinType)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[(K, (Iterable[V], Iterable[V]))] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .thn { dis =>
        RddStreamerUtils._coGroup(joinType)(
          left  = dis.rdd,
          right = that.thn(dis.asRDDBased).rdd) }
      .thn(RddStreamer.from)

  // ===========================================================================
  def asRDDBased[B >: A : ClassTag](that: Streamer[B]): RddStreamer[B] = asMeBased(that).asInstanceOf[RddStreamer[B]]

    // ---------------------------------------------------------------------------
    override def asMeBased[B >: A : ClassTag](that: Streamer[B]): Streamer[B] =
      that.tipe match {
        case StreamerType.ViewBased | StreamerType.IteratorBased =>
          sc.parallelize(that.toList).thn(RddStreamer.from)

        case StreamerType.RDDBased => that }

}

// ===========================================================================
object RddStreamer {
  def from[A: ClassTag](rdd: RDD[A]): Streamer[A] = new RddStreamer(rdd)
}

// ===========================================================================
