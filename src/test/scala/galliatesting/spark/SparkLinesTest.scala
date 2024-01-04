package galliatesting
package spark

import gallia._
import gallia.spark._

// ===========================================================================
object SparkLinesTest {

  def apply(name: String)(input: String): Unit = {
    val sc: SparkContext = galliaSparkContext(name)

    val res =
      sc
        .lines(input)
        .toUpperCase(_line)
        ._forceResult

    sc.stop()

    // ---------------------------------------------------------------------------
    res
      .ensuring(
        _ == bobjs(
            bobj(_line -> "FOO,BAZ"),
            bobj(_line -> "BAR1,1"),
            bobj(_line -> "BAR2,2"),
            bobj(_line -> ""))
          .forceAObjs) } }

// ===========================================================================
