package aptus.spark

import org.apache.spark.SparkContext

// ===========================================================================
/** poor man's caching */
object SparkContextCache {
  private var value: Option[SparkContext] = None

  // ---------------------------------------------------------------------------
  private def initializeOrReplace(sc: SparkContext): SparkContext = { value = Some(sc); sc }

    def initialize(sc: SparkContext): SparkContext = synchronized {
      require(value.isEmpty, value)

      initializeOrReplace(sc)
    }

    def replace(sc: SparkContext): SparkContext = synchronized {
      require(value.nonEmpty, value)

      initializeOrReplace(sc)
    }

  // ---------------------------------------------------------------------------
  def opt   : Option[SparkContext] = synchronized { value     }
  def force :        SparkContext  = synchronized { value.get }
}

// ===========================================================================
