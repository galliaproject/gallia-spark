package aptus.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// ===========================================================================
object SparkDriver {

  def context(
        url    : UrlString = DefaultMaster,
        name   : AppName   = DefaultAppName,
        managed: Boolean   = true)
    : SparkContext =
      synchronized {
        _cache
          .opt
          .getOrElse {
            createContext(url, name, managed) } }

    // ===========================================================================
    private def createContext(url: String, name: AppName, managed: Boolean): SparkContext = {
      //println("creating spark-context") // TODO: t210122092713 - proper logging

      val conf =
        new SparkConf()
          .setAppName(name)
          .setMaster(url)

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

}

// ===========================================================================
