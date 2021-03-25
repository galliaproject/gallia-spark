package gallia

import aptus._
import aptus.spark._
import org.apache.hadoop.io.compress._

// ===========================================================================
package object spark {
  type ClassTag[A]  = scala.reflect.ClassTag[A]

  type SparkContext = org.apache.spark.SparkContext

  type RDD     [A]  = org.apache.spark.rdd.RDD[A]
  type Streamer[A]  = gallia.data.multiple.Streamer[A]

  type Line = aptus.Line

  type RddStreamer[A] = gallia.data.multiple.streamer.RddStreamer[A]
  val  RddStreamer    = gallia.data.multiple.streamer.RddStreamer

  // ===========================================================================
  def galliaSparkContext(name: AppName = DefaultAppName): SparkContext = SparkDriver.context(name, managed = false)

  // ===========================================================================
  implicit class StartReadZFluency__(override val conf : gallia.io.in.StartReadZFluency) extends StartReadFluencyRDD
  implicit class HeadZ__            (override val headZ: gallia.HeadZ)                   extends gallia.heads.HeadZRdd

  // ---------------------------------------------------------------------------
  implicit class SparkContext_(sc: SparkContext) {
    def tsvWithHeader(path: String)(key1: KeyW, more: KeyW*): HeadS = in.tsvWithHeader(sc, path)(key1, more:_*)
  } 
  
  // ===========================================================================
  object logging {
    def setToWarn() { setTo(org.apache.log4j.Level.WARN) }
    def setTo(level: org.apache.log4j.Level) { aptus.spark.SparkLogging.setLoggingTo(level) }
  }
            
  // ===========================================================================
  object in {
  
	  def tsvWithHeader(sc: SparkContext, path: String)(key1: KeyW, more: KeyW*): HeadS =     
  			  in.RddInputLines(sc, path, Some(1))
  			  .thn(heads.Head.inputZ)
  			  .thn(tsvFromLine(key1, more:_*))    
  
      // ---------------------------------------------------------------------------
      private def tsvFromLine(key1: KeyW, more: KeyW*): HeadS => HeadS =
        _ .split        (_line).byTsv
          .untuplify1z  (_line).asNewKeys(KeyWz.from(key1, more))
          .unnestAllFrom(_line)
          
      // ===========================================================================
      case class RddInputLines(sc: SparkContext, inputPath: String, drop: Option[Int]) extends ActionIZd { // TODO: charset (t210121164950)/compression(t210121164951)
          def vldt   = Nil//TODO + check drop > 0 if provided + not "too big" (see t210312092358)
          def _meta  = Cls.Line
          def atomiz = _RddInputLines(sc, inputPath, drop) }
    
        // ===========================================================================
        case class _RddInputLines(sc: SparkContext, inputPath: String, drop: Option[Int]) extends AtomIZ {
          def naive: Option[Objs] = 
              lines(sc)(inputPath, drop)
                .thn(Objs.build)
                .as.some
    
          // ---------------------------------------------------------------------------
          private def lines(sc: SparkContext)(in: String, drop: Option[Int]): Streamer[Obj] = 
            sc.textFile(in)
              .thn(RddStreamer.from)        
              .map(line => obj(_line -> line))
              .thnOpt(drop)(n => _.drop(n))
        }
    }
      
  // ===========================================================================
  object out {
    var Compression: Class[_ <: CompressionCodec] = classOf[GzipCodec]  // TODO: t210121164951 - configurable compression
      //no compression: sc.hadoopConfiguration.set("mapred.output.compress", "false") + DefaultCodec

    // ---------------------------------------------------------------------------
    case class RddOutputZ(uriString: String) extends ActionZOb {
        def vldt   (c: Cls) = Nil //TODO
        def atomzos(c: Cls) = Seq(_RddOutputZ(uriString)) } // TODO: t210322101244 - also write schema too (coalesce to 1 first)            

      // ---------------------------------------------------------------------------    
      case class _RddOutputZ(uriString: String) extends AtomZO {
        def naive(z: Objs) {
          z .values
            .asInstanceOf[RddStreamer[Obj]] /* TODO: wrap error */
            .rdd
            .saveAsTextFile(uriString, Compression) } }      
  }

}

// ===========================================================================
