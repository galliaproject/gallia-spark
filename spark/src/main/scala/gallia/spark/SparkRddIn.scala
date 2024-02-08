package gallia
package spark

import aptus._

// ===========================================================================
object SparkRddIn { // TODO t210330110143 - p2 - align with core's io.in abstraction

  def rdd  (sc: SparkContext, schema: Cls, rdd: RDD[Obj]): HeadS = RddInputObjs (schema, rdd)             .pipe(heads.Head.inputZ)
  def lines(sc: SparkContext, path: String)              : HeadS = RddInputLines(sc, path, dropOpt = None).pipe(heads.Head.inputZ)

  // ---------------------------------------------------------------------------
  def jsonLines(sc: SparkContext, schema: Cls, path: String) : HeadS = RddInputJsonLines(sc, schema, path).pipe(heads.Head.inputZ)

  // ---------------------------------------------------------------------------
  def csvWithHeader(sc: SparkContext, path: String)(key1: KeyW, more: KeyW*): HeadS =
          RddInputLines(sc, path, dropOpt = Some(1))
            .pipe(heads.Head.inputZ)
            .pipe(csvFromLine(key1, more:_*))

      // ---------------------------------------------------------------------------
      def tsvWithHeader(sc: SparkContext, path: String)(key1: KeyW, more: KeyW*): HeadS =
          RddInputLines(sc, path, dropOpt = Some(1))
            .pipe(heads.Head.inputZ)
            .pipe(tsvFromLine(key1, more:_*))

    // ---------------------------------------------------------------------------
    private def csvFromLine(key1: KeyW, more: KeyW*): HeadS => HeadS =
      _ .filterOutEmptyLines(_line)
        .split              (_line).byCsv
        .deserialize1z      (_line).asNewKeys(KeyWz.from(key1, more))
        .unnestAllFrom      (_line)

    // ---------------------------------------------------------------------------
    private def tsvFromLine(key1: KeyW, more: KeyW*): HeadS => HeadS =
      _ .filterOutEmptyLines(_line)
        .split              (_line).byTsv
        .deserialize1z      (_line).asNewKeys(KeyWz.from(key1, more))
        .unnestAllFrom      (_line)

    // ===========================================================================
    case class RddInputLines(sc: SparkContext, inputPath: String, dropOpt: Option[Int]) extends ActionIZ01 { // TODO: charset (t210121164950)/compression(t210121164951)
        def vldt   = Nil//TODO + check drop > 0 if provided + not "too big" (see t210312092358)
        def _meta  = Cls.Line
        def atomiz = _RddInputLines(sc, inputPath, dropOpt) }

      // ===========================================================================
      case class _RddInputLines(sc: SparkContext, inputPath: String, dropOpt: Option[Int]) extends AtomIZ {
          /** IMPORTANT NOTE: 240104135138 - with scala 3, we HAVE to externalize it if we pass sc (to investigate) */
          def naive: Option[Objs] = _RddInputLines.naive(sc, inputPath, dropOpt) }

        // ---------------------------------------------------------------------------
        object _RddInputLines {
          def naive(sc: SparkContext, inputPath: String, dropOpt: Option[Int]): Option[Objs] =
            Utils.parseObjsOpt(sc, inputPath)(
                dropOpt, skipEmptyLines = true /* TODO: t240104142220 - limit to last one only? */) {
              x => gallia.obj(_line -> x) } }

    // ===========================================================================
    case class RddInputObjs(schema: Cls, rdd: RDD[Obj]) extends ActionIZ01 {
        def vldt   = Nil//TODO
        def _meta  = schema
        def atomiz = _RddInputObjs(rdd) }

      // ===========================================================================
      case class _RddInputObjs(rdd: RDD[Obj]) extends AtomIZ {
        def naive: Option[Objs] =
          new RddStreamer[Obj](rdd)
            .pipe(Objs.build)
            .in.some }

    // ===========================================================================
    case class RddInputJsonLines(sc: SparkContext, schema: Cls, inputPath: String) extends ActionIZ01 { // TODO: charset (t210121164950)/compression(t210121164951)
        def vldt   = Nil // TODO
        def _meta  = schema
        def atomiz = _RddInputJsonLines(
          sc, schema, inputPath) }

      // ===========================================================================
      case class _RddInputJsonLines(sc: SparkContext, schema: Cls, inputPath: String) extends AtomIZ {
          /** IMPORTANT NOTE: 240104135138 - with scala 3, we HAVE to externalize it if we pass sc (to investigate) */
          def naive: Option[Objs] = _RddInputJsonLines.naive(sc, inputPath, schema) }

        // ---------------------------------------------------------------------------
        object _RddInputJsonLines {

          def naive(sc: SparkContext, inputPath: String, schema: Cls): Option[Objs] =
            Utils.parseObjsOpt(sc, inputPath)(
                dropOpt = None, skipEmptyLines = true) {
              data.json.GsonToGalliaData.parseRecursively(schema, _) } }

  // ===========================================================================
  private object Utils {

    def parseObjsOpt(sc: SparkContext, inputPath: String)(dropOpt: Option[Int], skipEmptyLines: Boolean)(lineToObj: Line => Obj): Option[Objs] =
      inputPath
        .pype(sc.textFile(_, numPartitions(sc)))
        .pype(RddStreamer.from)

        // ---------------------------------------------------------------------------
        .pipeOpt(dropOpt)       (n => _.drop(n))                         // TODO: t210330110534 - as separate atom
        .pipeIf (skipEmptyLines)(_.flatMap(_.in.noneIf(_.trim.isEmpty))) // TODO: t210330110534 - as separate atom

        // ---------------------------------------------------------------------------
        .map (lineToObj)
        .pipe(Objs.build)
        .in.some }

}

// ===========================================================================
