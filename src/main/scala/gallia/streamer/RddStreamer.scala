package gallia
package streamer

import aptus.Anything_
import atoms.utils.SuperMetaPair
import gallia.spark._

// ===========================================================================
class RddStreamer[A: CWTT /* t210322130619 - generalize to Streamer + as WTT */](val rdd: RDD[A]) extends Streamer[A] {
  override val tipe = StreamerType.RDDBased

  private def sc = this.rdd.sparkContext

  // ---------------------------------------------------------------------------
  private         def _rewrap(x: RDD[A]) = RddStreamer.from(x)
  private[gallia] def _alter[B : CWTT](f: RDD[A] => RDD[B]): Streamer[B] = RddStreamer.from(f(rdd))

  // ===========================================================================
  private[gallia] def selfClosingIterator:                 Iterator[A] = rdd.toLocalIterator // TODO: ok? nothing to close?
  private[gallia] def closeabledIterator : aptus.CloseabledIterator[A] = aptus.CloseabledIterator.fromUncloseable(rdd.toLocalIterator) // TODO: confirm

  // ---------------------------------------------------------------------------
//override def toView  = ???//: ViewRepr[A] = iterator.toSeq.view - TODO: t210315114618 - causes odd compilation issues with gallia-spark, to investigate
  override def toList: List[A] = rdd.collect().toList

  // ===========================================================================
  override def     map[B : CWTT](f: A =>             B ): Streamer[B] = rdd.    map(f)(ctag[B]).pype(RddStreamer.from)
  override def flatMap[B : CWTT](f: A => gallia.Coll[B]): Streamer[B] = rdd.flatMap(f(_).asInstanceOf[gallia.SparkColl[B] /*FIXME:t210122102109*/])(ctag[B]).pype(RddStreamer.from)

  override def filter(p: A => Boolean): Streamer[A] = rdd.filter(p).pype(RddStreamer.from)
  override def find  (p: A => Boolean): Option  [A] = rdd.filter(p).take(1).pipe { x => if (x.assert(_.size <= 1).size == 1) Some(x.head) else None }

  @IntSize
  override def size: Int = rdd.count().toInt //FIXME: t210122094438
  
  override def isEmpty: Boolean = rdd.isEmpty

  // ---------------------------------------------------------------------------
  // FIXME: t210312092358 - if n is bigger than partition size (both take/drop); maybe offer "takeFew"/"dropFew" with a set max?
  override def take(n: Int): Streamer[A] = rdd.mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.take(n) else itr }(ctag[A]).pype(_rewrap)
  override def drop(n: Int): Streamer[A] = rdd.mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.drop(n) else itr }(ctag[A]).pype(_rewrap)
  // TODO: takeLast/dropLast(n): use rdd.getNumPartitions()

  // ---------------------------------------------------------------------------
  override def takeWhile(p: A => Boolean): Streamer[A] = aptus.illegalState("220721130550 - takeWhile unsupported with RDDs")
  override def dropWhile(p: A => Boolean): Streamer[A] = aptus.illegalState("220721130551 - dropWhile unsupported with RDDs")

  // TODO: t210122094456 - also off sample

  // ---------------------------------------------------------------------------
  override def reduce(op: (A, A) => A): A = rdd.reduce(op)

  // ===========================================================================
  override def distinct: Streamer[A] = _alter(_.distinct)
  // TODO: cheaper ensure distinct - t201126163157 - maybe sort followed by rdd.mapPartitions(_.sliding(size, step), preservesPartitioning)?

  // ===========================================================================
  override def sortBy[K](meta: SuperMetaPair[K])(f: A => K): Streamer[A] = //TODO: t210122094521 - allow setting numPartitions properly
    _alter(_.sortBy(f, numPartitions = gallia.spark.numPartitions(sc))(meta.ord, meta.ctag))

  // ===========================================================================
  override def groupByKey[K: CWTT, V: CWTT](implicit ev: A <:< (K, V)): Streamer[(K, List[V])] = {
    implicit val ctk: CT[K] = ctag[K]
    implicit val ctv: CT[V] = ctag[V]

    this.asInstanceOf[RddStreamer[(K, V)]]._alter { _.groupByKey.mapValues(_.toList) } }

  // ===========================================================================
  override def zip[B >: A : CWTT](that: Streamer[B], combiner: (B, B) => B): Streamer[B] =
    RddStreamer.from(
      (  (this.asInstanceOf[RddStreamer[B]].rdd zip // ensures same size already (else throws a SparkException)
          that.pype(this.asRDDBased)       .rdd))(ctag[B])
        .map(combiner.tupled)(ctag[B]))

  // ---------------------------------------------------------------------------
  override def union[B >: A : CWTT](that: Streamer[B]): Streamer[B] =
    RddStreamer.from(
      this.asInstanceOf[RddStreamer[B]].rdd ++
      that.pype(this.asRDDBased)       .rdd)

  // ===========================================================================
  override def join[K: CWTT, V: CWTT](joinType: JoinType, combiner: (V, V) => V)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[V] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .pype { dis =>
        implicit val ctk: CT[K] = ctag[K]
        implicit val ctv: CT[V] = ctag[V]

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
      .flatMap(RddStreamerUtils.postJoinCombining(combiner))(ctag[V])
      .pype(RddStreamer.from)

  // ---------------------------------------------------------------------------
  override def coGroup[K: CWTT, V: CWTT](joinType: JoinType)(that: Streamer[(K, V)])(implicit ev: A <:< (K, V)): Streamer[(K, (Iterable[V], Iterable[V]))] =
    this
      .asInstanceOf[RddStreamer[(K, V)]]
      .pype { self =>
        implicit val ctk: CT[K] = ctag[K]
        implicit val ctv: CT[V] = ctag[V]

        RddStreamerUtils._coGroup(joinType)(
          left  = self                 .rdd,
          right = self.asRDDBased(that).rdd) }
      .pype(RddStreamer.from)

  // ===========================================================================
  override def toViewBased    : Streamer[A] = ViewStreamer.from(toList)                                                                                // TODO: confirm closes everything that needs closing at the end of iteration?
  override def toIteratorBased: Streamer[A] = new DataRegenerationClosure[A] { def regenerate = () => closeabledIterator }.pipe(IteratorStreamer.from) // TODO: confirm closes everything that needs closing at the end of iteration?

  // ---------------------------------------------------------------------------
           def asRDDBased[B >: A : CWTT](that: Streamer[B]): RddStreamer[B] = toMeBased(that).asInstanceOf[RddStreamer[B]]
  override def toMeBased [B >: A : CWTT](that: Streamer[B]): Streamer   [B] = that.tipe match {
    case StreamerType.ViewBased     => that.toList.pype(sc.parallelize(_, numPartitions(sc))(ctag[B])).pype(RddStreamer.from)
    case StreamerType.IteratorBased => aptus.illegalState(data.multiple.CantMixIteratorAndRddProcessing)
    case StreamerType.RDDBased      => that }
}

// ===========================================================================
object RddStreamer {
  def from[A: CWTT](rdd: RDD[A]): Streamer[A] = new RddStreamer(rdd) }

// ===========================================================================
