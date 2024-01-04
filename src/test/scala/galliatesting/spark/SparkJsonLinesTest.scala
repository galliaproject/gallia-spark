package galliatesting
package spark

import gallia._
import gallia.spark._

// ===========================================================================
object SparkJsonLinesTest {

  def apply(name: String)(input: String): Unit = {
    val sc: SparkContext = galliaSparkContext(name)

    val res =
      sc
        .jsonLines(cls("foo".string, "baz".int))(input)
        .toUpperCase("foo")
        ._forceResult

    sc.stop()

    // ---------------------------------------------------------------------------
    res
      .ensuring(
        _ == bobjs(
            bobj("foo" -> "BAR1", "baz" -> 1),
            bobj("foo" -> "BAR2", "baz" -> 2))
          .forceAObjs) } }

// ===========================================================================
