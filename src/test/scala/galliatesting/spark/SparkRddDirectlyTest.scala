package galliatesting
package spark

import gallia._
import gallia.spark._

// ===========================================================================
object SparkRddDirectlyTest {

  def apply(name: String)(input: String): Unit = {
// read avro with spark sql then turn it into an rdd; or read it all in memory with gallia.avro
    val sc: SparkContext = galliaSparkContext(name)

    val objs: RDD[Obj] = sc.textFile(input).map(x => obj(_line -> x))

    val res =
      sc.rdd(schema = Cls.Line, rdd = objs)
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
