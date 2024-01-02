package gallia
package spark

import org.apache.hadoop.io.compress._
import aptus.String_

// ===========================================================================
object SparkRddOut { // t210330110144 - p2 - align with core's io.out abstraction
    var Compression: Class[_ <: CompressionCodec] = classOf[GzipCodec]  // TODO: t210121164951 - configurable compression
      //no compression: sc.hadoopConfiguration.set("mapred.output.compress", "false") + DefaultCodec

    // ---------------------------------------------------------------------------
    case class RddOutputZ(uriString: String) extends ActionZOb {
        def vldt   (c: Cls) = Nil //TODO
        def atomzos(c: Cls) = Seq(
            _SchemaRddOutput(c, uriString, gallia.atoms.AtomsXO.DefaultSchemaSuffix),
            _RddOutputZ(uriString)) }

      // ---------------------------------------------------------------------------
      case class _RddOutputZ(uriString: String) extends AtomZO {
        def naive(z: Objs): Unit = { rdd(z).saveAsTextFile(uriString, Compression) } }

      // ---------------------------------------------------------------------------
      case class _SchemaRddOutput(c: Cls, uriString: String/*, urlLike: UrlLike*/, suffix: String) extends AtomZO {
        def naive(z: Objs): Unit = {
          rdd(z)
            .sparkContext
            .parallelize(Seq(c .formatPrettyJson /* TODO: t210128103821 - format configurable */), numSlices = 1)
            .saveAsTextFile(uriString.append(suffix)) } } // TODO: t210326155456 - any way not to write it as part-00000?

      // ===========================================================================
      private def rdd(z: Objs): RDD[Obj] =
        z .values
          .asInstanceOf[RddStreamer[Obj]] /* TODO: wrap error */
          .rdd }

// ===========================================================================
