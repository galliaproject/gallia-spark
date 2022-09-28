package gallia
package spark

import aptus.spark.SparkDriver

import gallia.io.in._

// ===========================================================================
trait StartReadFluencyRDD { val conf: StartReadZFluency

    def rdd: StartReadZFluency = rdd(SparkDriver.context())

    // ---------------------------------------------------------------------------
    def rdd(sc: SparkContext): StartReadZFluency = { //TODO:  t210122101757 - cache sc
      gallia.spark.registerSparkContext(sc)

      conf
    }

  }

// ===========================================================================
