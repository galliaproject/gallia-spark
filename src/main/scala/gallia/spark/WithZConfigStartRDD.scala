package gallia
package spark

import aptus.spark.SparkDriver

import gallia.io.in._

// ===========================================================================
trait StartReadFluencyRDD { val conf: StartReadZFluency

    def rdd: StartReadZFluency = rdd(SparkDriver.context())

    // ---------------------------------------------------------------------------
    def rdd(sc: SparkContext): StartReadZFluency = {
     //FIXME: t210122101756 - address hack
     //TODO:  t210122101757 - cache sc
      gallia.io.in.InputUrlLike.hackOpt =
        Some(
            StartReadFluencyRDD.streamLinesViaRDD(sc))

      conf
    }

  }

  // ===========================================================================
  object StartReadFluencyRDD {

    private def streamLinesViaRDD(input: InputUrlLike): Streamer[Line] =
        SparkDriver
          .context()
          .pipe(streamLinesViaRDD(_)(input))

    // ---------------------------------------------------------------------------
    private def streamLinesViaRDD(sc: SparkContext)(input: InputUrlLike): Streamer[Line] =
      sc
        .textFile(input._inputString) // TODO: charset (t210121164950)/compression(t210121164951)
        .pype(RddStreamer.from)
  }

// ===========================================================================
