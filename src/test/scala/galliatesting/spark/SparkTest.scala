package galliatesting
package spark

import utest._
import aptus._

// ===========================================================================
object SparkTest extends TestSuite  { // provided lib: c220519162721@sbt
  private val  CsvInputFile  = "/data/test/test.csv" // input is: foo,baz\nbar1,1\nbar2,2
  private val JsonlInputFile = "/data/test/test.jsonl"
  private val  AvroInputFile = "/data/test/serialization/avro/episodes.avro"

//FIXME: t240102135348 - must accept trailing newline...

  // ---------------------------------------------------------------------------
  val tests = Tests {

    //make sure spark works from here irrespective of gallia
    test("basic spark (non gallia)") {
      NonGalliaSparkTest(
        in  = CsvInputFile,
        out = "/tmp/spark_test.csv") }

    // ===========================================================================
    test("spark-csv")(SparkCsvTest("spark-csv")(CsvInputFile))

    // ---------------------------------------------------------------------------
    test("spark-lines-plain")(SparkLinesTest("spark-lines-plain")(CsvInputFile))
    test("spark-lines-gz"   )(SparkLinesTest("spark-lines-gz"   )(CsvInputFile.dot("gz" )))
    test("spark-lines-bz2"  )(SparkLinesTest("spark-lines-bz2"  )(CsvInputFile.dot("bz2")))

    // ---------------------------------------------------------------------------
    test("spark-rdd-directly")(SparkRddDirectlyTest("spark-rdd-directly")(CsvInputFile))

    // ---------------------------------------------------------------------------
    test("spark-jsonl")(SparkJsonLinesTest("spark-jsonl")(JsonlInputFile))

    // ---------------------------------------------------------------------------
    //test("spark-avro")(SparkAvroTest("spark-avro")(AvroInputFile)) - WIP

    // ---------------------------------------------------------------------------
    test("spark-register")(galliaSparkRegister("spark-register")) }

  // ===========================================================================
  private def galliaSparkRegister(name: String): Unit = {
    import gallia.spark._
    val sc: SparkContext = galliaSparkContext(name)
    registerSparkContext(sc) } // data_rdd = sqlContext.read.parquet(filename).rdd

}

// ===========================================================================
