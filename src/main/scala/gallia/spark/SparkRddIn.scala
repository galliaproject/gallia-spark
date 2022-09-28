package gallia
package spark

import aptus._

// ===========================================================================
object SparkRddIn { // TODO t210330110143 - p2 - align with core's io.in abstraction

  def rdd  (sc: SparkContext, schema: Cls, rdd: RDD[Obj]): HeadS = RddInputObjs (sc, schema, rdd)      .pipe(heads.Head.inputZ)
  def lines(sc: SparkContext, path: String)              : HeadS = RddInputLines(sc, path, drop = None).pipe(heads.Head.inputZ)

  // ---------------------------------------------------------------------------
  def jsonLines(sc: SparkContext, schema: Cls, path: String) : HeadS = RddInputJsonLines(sc, schema, path).pipe(heads.Head.inputZ)

  // ---------------------------------------------------------------------------
  def csvWithHeader(sc: SparkContext, path: String)(key1: KeyW, more: KeyW*): HeadS =
          RddInputLines(sc, path, drop = Some(1))
            .pipe(heads.Head.inputZ)
            .pipe(csvFromLine(key1, more:_*))

      // ---------------------------------------------------------------------------
      def tsvWithHeader(sc: SparkContext, path: String)(key1: KeyW, more: KeyW*): HeadS =
          RddInputLines(sc, path, drop = Some(1))
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
    case class RddInputLines(sc: SparkContext, inputPath: String, drop: Option[Int]) extends ActionIZd { // TODO: charset (t210121164950)/compression(t210121164951)
        def vldt   = Nil//TODO + check drop > 0 if provided + not "too big" (see t210312092358)
        def _meta  = Cls.Line
        def atomiz = _RddInputLines(sc, inputPath, drop) }

      // ===========================================================================
      case class _RddInputLines(sc: SparkContext, inputPath: String, drop: Option[Int]) extends AtomIZ {
        def naive: Option[Objs] =
          sc
            .textFile(inputPath, numPartitions(sc))
            .pype(RddStreamer.from)
            .map(line => obj(_line -> line))
            .pipeOpt(drop)(n => _.drop(n)) // TODO: t210330110534 - as separate atom
            .pipe(Objs.build)
            .in.some }

    // ===========================================================================
    case class RddInputObjs(sc: SparkContext, schema: Cls, rdd: RDD[Obj]) extends ActionIZd {
        def vldt   = Nil//TODO
        def _meta  = schema
        def atomiz = _RddInputObjs(sc, rdd) }

      // ===========================================================================
      case class _RddInputObjs(sc: SparkContext, rdd: RDD[Obj]) extends AtomIZ {
        def naive: Option[Objs] =
          new RddStreamer[Obj](rdd)
            .pipe(Objs.build)
            .in.some }

    // ===========================================================================
    case class RddInputJsonLines(sc: SparkContext, schema: Cls, inputPath: String) extends ActionIZd { // TODO: charset (t210121164950)/compression(t210121164951)
        def vldt   = Nil // TODO
        def _meta  = schema
        def atomiz = _RddInputJsonLines(sc,
          schema//.fields.head.info.union.head.multiple.toString
          , inputPath) }

      // ===========================================================================
      case class _RddInputJsonLines(sc: SparkContext, schema: Cls, inputPath: String) extends AtomIZ {
          def naive: Option[Objs] = _RddInputJsonLines.naive(sc, inputPath, schema) }

        // ---------------------------------------------------------------------------
        object _RddInputJsonLines {

          def naive(sc: SparkContext, inputPath: String, schema: Cls): Option[Objs] =
            inputPath
              .pype(sc.textFile(_, numPartitions(sc)))
              .pype(RddStreamer.from)
              .flatMap { line =>
                if (line.trim.isEmpty) None
                else                   Some(data.json.GsonToObj.fromObjectString(line)) }
              .map(data.json.GsonToGalliaData.convertRecursively(schema))
              .pipe(Objs.build)
              .in.some }

}

// ===========================================================================