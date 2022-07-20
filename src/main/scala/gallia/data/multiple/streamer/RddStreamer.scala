package gallia
package data.multiple.streamer

import aptus.Anything_
import gallia.spark._

// ===========================================================================
class RddStreamer[A: ClassTag /* t210322130619 - generalize to Streamer + as WTT */](val rdd: RDD[A]) extends Streamer[A] {
  override val tipe = StreamerType.RDDBased

  private def sc = this.rdd.sparkContext

  // ---------------------------------------------------------------------------
  private         def _rewrap(x: RDD[A]) = RddStreamer.from(x)
  private[gallia] def _modifyUnderlyingRdd(f: RDD[A] => RDD[A]): Streamer[A] = RddStreamer.from(f(rdd))

  // ---------------------------------------------------------------------------
  def egal(that: Streamer[A]): Boolean = ??? // TODO

  // ===========================================================================
  private[gallia] def selfClosingIterator:                 Iterator[A] = rdd.toLocalIterator // TODO: ok? nothing to close?
  private[gallia] def closeabledIterator : aptus.CloseabledIterator[A] = aptus.CloseabledIterator.fromUncloseable(rdd.toLocalIterator) // TODO: confirm

  // ---------------------------------------------------------------------------
//override def toView  = ???//: ViewRepr[A] = iterator.toSeq.view - TODO: t210315114618 - causes odd compilation issues with gallia-spark, to investigate
  override def toList  : List    [A] = rdd.collect().toList

  // ===========================================================================
  override def     map[B : ClassTag](f: A =>             B ): Streamer[B] = rdd.    map(f).pype(RddStreamer.from)
  override def flatMap[B : ClassTag](f: A => gallia.Coll[B]): Streamer[B] = rdd.flatMap(f(_).asInstanceOf[gallia.SparkColl[B] /*FIXME:t210122102109*/]).pype(RddStreamer.from)

  override def filter(p: A => Boolean): Streamer[A] = rdd.filter(p).pype(RddStreamer.from)
  override def find  (p: A => Boolean): Option  [A] = rdd.filter(p).take(1).pipe { x => if (x.assert(_.size <= 1).size == 1) Some(x.head) else None }

  @gallia.NumberAbstraction
  override def size: Int = rdd.count().toInt//FIXME: t210122094438

  override def isEmpty: Boolean = rdd.isEmpty

  // ---------------------------------------------------------------------------
  // TODO: t210122094456 - also sample
  // FIXME: t210312092358 - if n is bigger than partition size (both take/drop); maybe offer "takeFew"/"dropFew" with a set max?
  override def take(n: Int): Streamer[A] = rdd.mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.take(n) else itr }.pype(_rewrap)
  override def drop(n: Int): Streamer[A] = rdd.mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.drop(n) else itr }.pype(_rewrap)
  // TODO: takeLast/dropLast(n): use rdd.getNumPartitions()

override def takeWhile(p: A => Boolean): Streamer[A] = ???
override def dropWhile(p: A => Boolean): Streamer[A] = ???

  override def reduce(op: (A, A) => A): A = rdd.reduce(op)

  // ===========================================================================
  override def distinct: Streamer[A] = _modifyUnderlyingRdd(_.distinct)
  // TODO: ensure distinct - t201126163157 - maybe sort followed by rdd.mapPartitions(_.sliding(size, step), preservesPartitioning)?

  // ===========================================================================
  override def sortBy[K](meta: atoms.utils.SuperMetaPair[K])(f: A => K): Streamer[A] =
    _modifyUnderlyingRdd(_.sortBy(f)(meta.ord, meta.ctag)) //TODO: t210122094521 - allow setting numPartitions

  // ===========================================================================
  override def groupByKey[K: ClassTag, V: ClassTag](implicit ev: A <:< (K, V)): Streamer[(K, List[V])] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .rdd
      .groupByKey
      .mapValues(_.toList)
      .pype(RddStreamer.from)

  // ===========================================================================
  override def zip  [B >: A : ClassTag](that: Streamer[B], combiner: (B, B) => B): Streamer[B] = ??? // TODO
  override def union[B >: A : ClassTag](that: Streamer[B]): Streamer[B] =
    RddStreamer.from(
      this.asInstanceOf[RddStreamer[B]].rdd ++
      that.pype(this.asRDDBased)       .rdd)

  // ===========================================================================
  override def join[K: ClassTag, V: ClassTag](joinType: JoinType, combiner: (V, V) => V)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[V] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .pype { dis =>
        that.tipe match {
          case StreamerType.ViewBased => // TODO: t210322111234 - [res] - determine if using hash join is implicit (if Seq) or explicit (via conf), or a combination
            RddStreamerUtils
              ._hashJoin(joinType)(
                left  = dis.rdd,
                right = that.pype(dis.asRDDBased).rdd) // TODO: t210322110948 - wasteful: to broadcast directly?
          case _ =>
            RddStreamerUtils
              ._join(joinType)(
                left  = dis.rdd,
                right = that.pype(dis.asRDDBased).rdd) } }
      .flatMap(RddStreamerUtils.postJoinCombining(combiner))
      .pype(RddStreamer.from)

  // ---------------------------------------------------------------------------
  override def coGroup[K: ClassTag, V: ClassTag](joinType: JoinType)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[(K, (Iterable[V], Iterable[V]))] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .pype { dis =>
        RddStreamerUtils._coGroup(joinType)(
          left  = dis.rdd,
          right = that.pype(dis.asRDDBased).rdd) }
      .pype(RddStreamer.from)

  // ===========================================================================
  override def toViewBased    : Streamer[A] =     ViewStreamer.from(toList)             // TODO: confirm closes everything that needs closing at the end of iteration?
  override def toIteratorBased: Streamer[A] = IteratorStreamer.from(closeabledIterator) // TODO: confirm closes everything that needs closing at the end of iteration?

  // ---------------------------------------------------------------------------
           def asRDDBased[B >: A : ClassTag](that: Streamer[B]): RddStreamer[B] = toMeBased(that).asInstanceOf[RddStreamer[B]]
  override def toMeBased [B >: A : ClassTag](that: Streamer[B]): Streamer   [B] =
    that.tipe match {
      case StreamerType.ViewBased | StreamerType.IteratorBased => RddStreamer.from(sc.parallelize(that.toList))
      case StreamerType.RDDBased                               => that }

}

// ===========================================================================
object RddStreamer {
  def from[A: ClassTag](rdd: RDD[A]): Streamer[A] = new RddStreamer(rdd)
}

// ===========================================================================
