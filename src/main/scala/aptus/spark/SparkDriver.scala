package aptus.spark

import util.chaining._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// ===========================================================================
object SparkDriver {

  def context(
        name   : AppName   = DefaultAppName,
        managed: Boolean   = true)
    : SparkContext =
      synchronized {
        _cache
          .opt
          .getOrElse {
            createContext(name, managed) } }

    // ===========================================================================
    private def createContext(name: AppName, managed: Boolean): SparkContext = {
      //println("creating spark-context") // TODO: t210122092713 - proper logging

      val conf =
        new SparkConf()
          .setAppName(name)
          .pipe(setMasterBasedOnMode)

      val sc = new SparkContext(conf)

      if (managed) {
        _cache.initialize(sc)

        sys.addShutdownHook {
          // TODO: or as "whatever is in cache?"

          //println("stopping spark-context")
          sc.stop() }
      }

      sc
    }

  // ===========================================================================
  /* TODO: t210322100113 - confirm/investigate:
   * 
   *   EMR/yarn expects no master to be set, but local standalone fails without one ("SparkException: A master URL must be set in your configuration")
   *   
   *   spark.submit.deployMode is supposed to be:
   *       either "client" or "cluster", Which means to launch driver program locally ("client") or remotely ("cluster") on one of the nodes inside the cluster.
   *     but in my case it's not set (None) when run locally (java.util.NoSuchElementException: spark.submit.deployMode),
   *       and it is set to "client" when running on EMR...
   *     see https://stackoverflow.com/questions/61137635/how-to-detect-that-pyspark-is-running-on-local-machine
   */
  private def setMasterBasedOnMode(conf: SparkConf): SparkConf =
    if (conf.getOption("spark.submit.deployMode").isEmpty) conf.setMaster(DefaultMaster) // TODO: t210322101134 allow providing non-default
    else conf

}

// ===========================================================================
