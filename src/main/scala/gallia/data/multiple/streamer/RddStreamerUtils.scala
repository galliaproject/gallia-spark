package gallia.data.multiple.streamer

import aptus.Anything_

import gallia.spark._

// ===========================================================================
private object RddStreamerUtils {

  def _join[K: ClassTag, V: ClassTag](joinType: JoinType)(left: RDD[(K, V)], right: RDD[(K, V)]): RDD[(K, (Option[V], Option[V]))] =
      joinType match {
          case JoinType.full  => left. fullOuterJoin(right)
          case JoinType.left  => left. leftOuterJoin(right).map { x => (x._1, (Some(x._2._1),      x._2._2 )) }
          case JoinType.right  => left.rightOuterJoin(right).map { x => (x._1, (     x._2._1 , Some(x._2._2))) }
          case JoinType.inner => left.          join(right).map { x => (x._1, (Some(x._2._1), Some(x._2._2))) } }

    // ---------------------------------------------------------------------------
    def _coGroup[K: ClassTag, V: ClassTag](joinType: JoinType)(left: RDD[(K, V)], right: RDD[(K, V)]): RDD[(K, (Iterable[V], Iterable[V]))] =
      left
        // TODO: t201126111306 - confirm no better way; no {left,right,inner}CoGroup available it seems
        //   note: using the {left,right,inner}OuterJoin here would force us to redo a re-grouping
        .cogroup(right)
        .thn { coGrouped =>
          joinType match {
              case JoinType.full  => coGrouped
              case JoinType.left  => coGrouped.filter(_._2._1.nonEmpty)
              case JoinType.right  => coGrouped                         .filter(_._2._2.nonEmpty)
              case JoinType.inner => coGrouped.filter(_._2._1.nonEmpty).filter(_._2._2.nonEmpty) } } // TODO: t210122095106 - confirm no performance impact

  // ===========================================================================
  def joinCombining[V](combiner: (V, V) => V)(tuple: (_, (Option[V], Option[V]))): Option[V] = {
    val (_/* kept in left */, (left: Option[V], right: Option[V])) = tuple

         if (left.isEmpty) right
    else if (right.isEmpty) left
    else for { l <- left; r <- right }
      yield { combiner(l, r) } // 201126124701 - can't both be empty (by design)
  }

}

// ===========================================================================
