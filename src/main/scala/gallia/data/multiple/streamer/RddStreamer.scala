package gallia.data.multiple.streamer

import aptus.Anything_

import gallia._
import gallia.spark._

// ===========================================================================
class RddStreamer[A](sc: SparkContext, val rdd: RDD[A]) extends Streamer[A] {
  val tipe = StreamerType.RDDBased

  // ---------------------------------------------------------------------------
  private      def _rewrap(x: RDD[A]) = RddStreamer.from(sc)(x)
  private[gallia] def _modifyUnderlyingRdd(f: RDD[A] => RDD[A]): Streamer[A] = RddStreamer.from(sc)(f(rdd))

  // ---------------------------------------------------------------------------
  def egal(that: Streamer[A]): Boolean = ??? // TODO

  def iteratorAndCloseable: (Iterator[A], java.io.Closeable) = (iterator, ??? /* see t210115104555 */)

  def iterator: Iterator[A] = rdd.toLocalIterator // TODO: ok?
  def toView  : ViewRepr[A] = iterator.toSeq.view
  def toList  : List    [A] = rdd.collect().toList

  def reduce(op: (A, A) => A): A = rdd.reduce(op)

  def     map[B : ClassTag](f: A =>      B ): Streamer[B] = rdd.    map(f).thn(RddStreamer.from(sc))
  def flatMap[B : ClassTag](f: A => Coll[B]): Streamer[B] = rdd.flatMap(f(_).asInstanceOf[SparkColl[B] /*FIXME:t210122102109*/]).thn(RddStreamer.from(sc))

  def filter(p: A => Boolean): Streamer[A] = rdd.filter(p).thn(RddStreamer.from(sc))

  @gallia.NumberAbstraction
  def size: Int = rdd.count().toInt//FIXME: t210122094438

  def  isEmpty: Boolean = rdd.isEmpty

  // TODO: t210122094456 - also sample
  def take(n: Int): Streamer[A] = ???//x.take(n).thn(RddStreamer.from)
  def drop(n: Int): Streamer[A] = ???//x.drop(n).thn(RddStreamer.from)

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
      .thn(RddStreamer.from(sc))

  // ===========================================================================
  def union[B >: A : ClassTag](that: Streamer[B]): Streamer[B] =
    RddStreamer.from(sc)(
      this.asInstanceOf[RddStreamer[B]].rdd ++
      that.thn(this.asRDDBased)        .rdd)

  // ===========================================================================
  def join[K: ClassTag, V: ClassTag](joinType: JoinType, combiner: (V, V) => V)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[V] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .thn { dis =>
        _utils._join(joinType)(
          left  = dis.rdd,
          right = that.thn(dis.asRDDBased).rdd) }
      .flatMap(_utils.joinCombining(combiner))
      .thn(RddStreamer.from(sc))

  // ---------------------------------------------------------------------------
  def coGroup[K: ClassTag, V: ClassTag](joinType: JoinType)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[(K, (Iterable[V], Iterable[V]))] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .thn { dis =>
        _utils._coGroup(joinType)(
          left  = dis.rdd,
          right = that.thn(dis.asRDDBased).rdd) }
      .thn(RddStreamer.from(sc))

  // ===========================================================================
  def asRDDBased[B >: A : ClassTag](that: Streamer[B]): RddStreamer[B] = asMeBased(that).asInstanceOf[RddStreamer[B]]

    // ---------------------------------------------------------------------------
    override def asMeBased[B >: A : ClassTag](that: Streamer[B]): Streamer[B] =
      that.tipe match {
          case StreamerType.ViewBased | StreamerType.IteratorBased =>
            sc.parallelize(that.toList).thn(RddStreamer.from(sc))

          case StreamerType.RDDBased => that }

}

// ===========================================================================
object RddStreamer {
  def from[A](sc: SparkContext)(rdd: RDD[A]): Streamer[A] = new RddStreamer(sc, rdd)
}

// ===========================================================================
