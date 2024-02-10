package galliatesting
package spark

import util.chaining._
import utest._
import aptus._
import gallia._
import org.apache.spark._

// ===========================================================================
/** make sure spark works from here irrespective of gallia */
object NonGalliaSparkTest {

  def apply(in: String, out: String): Unit = {
    assert(out.startsWith("/tmp/"))
    if (out.path.exists()) {
      out.path.removeFiles()
      out.path.removeEmptyDir() }

    // ---------------------------------------------------------------------------
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("my-spark")
        .setMaster("local"))//local[*, 3]")

    println("spark_version = " + sc.version)

    sc
      .textFile(in, minPartitions = 1)
      .map(_.toUpperCase)
      .saveAsTextFile(out)

    sc.stop()

    // ---------------------------------------------------------------------------
    "/tmp/spark_test.csv/part-00000".readFileLines().ensuring(_ == List(
      "FOO,BAZ",
      "BAR1,1",
      "BAR2,2",
      "")) }

}

// ===========================================================================
