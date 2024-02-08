package galliatesting
package spark

import utest._
import aptus._

// ===========================================================================
/*
 must use:
   export JAVA_OPTS="--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED"
   sbt [...]
 to address the likes of:
   java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$
 TODO: t240208113030 - any way around it from testing framework?
 */
object SparkTest extends TestSuite  { // provided lib: c220519162721@sbt
  private val ParentDir: String = getClass.getResource(s"/spark").getPath

  // ---------------------------------------------------------------------------
  private val  CsvInputFile  = ParentDir / "test.csv" // input is: foo,baz\nbar1,1\nbar2,2\n
  private val JsonlInputFile = ParentDir / "test.jsonl"

  // ---------------------------------------------------------------------------
  val tests = Tests {

    //make sure spark works from here irrespective of gallia
    test("basic spark (non gallia)") {
      NonGalliaSparkTest(
        in  = CsvInputFile,
        out = "/tmp/spark_test.csv") }

    // ===========================================================================
    test("spark-csv")                    (SparkCsvTest("spark-csv")(CsvInputFile))
    test("spark-csv-no-trailing-newline")(SparkCsvTest("spark-csv-no-trailing-newline")(ParentDir / "test_no_trailing_newline.csv"))

    // ---------------------------------------------------------------------------
    test("spark-lines-plain")(SparkLinesTest("spark-lines-plain")(CsvInputFile))
    test("spark-lines-gz"   )(SparkLinesTest("spark-lines-gz"   )(CsvInputFile.dot("gz" )))
    test("spark-lines-bz2"  )(SparkLinesTest("spark-lines-bz2"  )(CsvInputFile.dot("bz2")))

    // ---------------------------------------------------------------------------
    test("spark-rdd-directly")(SparkRddDirectlyTest("spark-rdd-directly")(CsvInputFile))

    // ---------------------------------------------------------------------------
    test("spark-jsonl")(SparkJsonLinesTest("spark-jsonl")(JsonlInputFile))

    // ---------------------------------------------------------------------------
    //test("spark-avro")(SparkAvroTest("spark-avro")(ParentDir / "serialization/avro/episodes.avro")) - WIP

    // ---------------------------------------------------------------------------
    test("spark-register")(galliaSparkRegister("spark-register"))
  }

  // ===========================================================================
  private def galliaSparkRegister(name: String): Unit = {
    import gallia.spark._
    val sc: SparkContext = galliaSparkContext(name)
    registerSparkContext(sc) } // data_rdd = sqlContext.read.parquet(filename).rdd

}

// ===========================================================================
