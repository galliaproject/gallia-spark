package gallia
package streamer

import org.apache.spark.broadcast.Broadcast
import aptus._
import gallia.spark._

// ===========================================================================
private object RddStreamerHashJoin {

  def innerHashJoinWithLeftBias[K: ClassTag, V: ClassTag](
        big  : RDD [(K, V)],
        small: RDD [(K, V)])
      : RDD[(K, (V, V))] = {   
    val map: Map[K, Seq[V]] = small.collect().toSeq.groupByKey

    val broadcast: Broadcast[collection.Map[K, Seq[V]]] = big.sparkContext.broadcast(map)

    big
      .mapPartitions(
        _.flatMap { case (key, leftValue) =>          
          broadcast
            .value.get(key).toSeq
            .flatMap {
              _.map { rightValue =>
                (key, (leftValue, rightValue)) } } },
        preservesPartitioning = true) }

  // ===========================================================================
  def leftHashJoin[K: ClassTag, V: ClassTag](
        big  : RDD [(K, V)],
        small: RDD [(K, V)])
      : RDD[(K, (V, Option[V]))] = {
    val map: Map[K, Seq[V]] = small.collect().toSeq.groupByKey
    
    val broadcast: Broadcast[collection.Map[K, Seq[V]]] = big.sparkContext.broadcast(map)

    big
      .mapPartitions(
        _.flatMap { case (key, leftValue) =>       
          broadcast
            .value.get(key)
             match {
              case None              => Seq(                            (key, (leftValue, None)))
              case Some(rightValues) => rightValues.map { rightValue => (key, (leftValue, Some(rightValue))) } } },
        preservesPartitioning = true) }

  // ===========================================================================
  def rightHashJoin[K: ClassTag, V: ClassTag](
        small: RDD [(K, V)],
        big  : RDD [(K, V)])
      : RDD[(K, (Option[V], V))] = {
    val map: Map[K, Seq[V]] = small.collect().toSeq.groupByKey
    
    val broadcast: Broadcast[collection.Map[K, Seq[V]]] = big.sparkContext.broadcast(map)

    big
      .mapPartitions(
        _.flatMap { case (key, rightValue) =>       
          broadcast
            .value.get(key)
             match {
              case None             => Seq(                          (key, (None,            rightValue)))
              case Some(leftValues) => leftValues.map { leftValue => (key, (Some(leftValue), rightValue)) } } },
        preservesPartitioning = true) }
  
  // ===========================================================================
  object PreGrouped {

    def innerHashJoinWithLeftBias[K: ClassTag, V: ClassTag](
          big  : RDD [(K, V)],
          small: RDD [(K, V)])
        : RDD[(K, (V, V))] = {
      val broadcast: Broadcast[collection.Map[K, V]] =
        big.sparkContext.broadcast(
          small.collectAsMap())
  
      big
        .mapPartitions(
          _.flatMap { case (key, leftValue) =>          
            broadcast
              .value.get(key)
              .map { rightValue =>
                (key, (leftValue, rightValue)) } },
          preservesPartitioning = true) }
  
    // ===========================================================================
    def leftHashJoin[K: ClassTag, V: ClassTag](
          big  : RDD [(K, V)],
          small: RDD [(K, V)])
        : RDD[(K, (V, Option[V]))] = {
      val broadcast: Broadcast[collection.Map[K, V]] =
        big.sparkContext.broadcast(
          small.collectAsMap())
  
      big
        .mapPartitions(
          _.map { case (key, leftValue) =>
            (key, (leftValue, broadcast.value.get(key))) },
          preservesPartitioning = true) }
  
    // ===========================================================================
    def rightHashJoin[K: ClassTag, V: ClassTag](
          small: RDD [(K, V)],
          big  : RDD [(K, V)])
        : RDD[(K, (Option[V], V))] = {
      val broadcast: Broadcast[collection.Map[K, V]] =
        big.sparkContext.broadcast(
          small.collectAsMap())
  
      big
        .mapPartitions(
          _.map { case (key, rightValue) =>
            (key, (broadcast.value.get(key), rightValue)) },
          preservesPartitioning = true) } } }

// ===========================================================================
