package gallia
package spark

// ===========================================================================
object SparkRddUtils {

  def sparkRddHack(sc: SparkContext): SparkRddHack =
    new SparkRddHack(
      streamRddLines =
        _._inputString // TODO: charset (t210121164950)/compression(t210121164951) - note compression for gz/bz2 seems to be handled automatically
          .pype(sc.textFile(_, numPartitions(sc)))
          .pype(RddStreamer.from),
      toRddStreamer =
        _ .toList
          .pype(sc.parallelize(_, numPartitions(sc)))
          .pype(new RddStreamer[gallia.Obj](_)) )

}

// ===========================================================================
