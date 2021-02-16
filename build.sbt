// gallia-spark

// ===========================================================================
// TODO: inherit these from core

// ---------------------------------------------------------------------------
lazy val scala213 = "2.13.4"
lazy val scala212 = "2.12.13"

// ---------------------------------------------------------------------------
lazy val supportedScalaVersions = List(scala213, scala212)

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    name               := "gallia-spark",
    version            := "0.1.0",
    scalaVersion       := supportedScalaVersions.head,
    crossScalaVersions := supportedScalaVersions)
  .dependsOn(RootProject(file("../gallia-core")))

// ===========================================================================
// TODO: more + inherit from core
scalacOptions in Compile ++=
  Seq("-Ywarn-value-discard") ++ 
  (scalaBinaryVersion.value match {
    case "2.13" => Seq("-Ywarn-unused:imports")
    case _      => Seq("-Ywarn-unused-import" ) })

// ===========================================================================
lazy val sparkVersion213 = "3.2.0-20210212.011521-17" // per https://repository.apache.org/content/repositories/snapshots/org/apache/spark/spark-core_2.13/3.2.0-SNAPSHOT/, see http://apache-spark-developers-list.1001551.n3.nabble.com/FYI-Scala-2-13-Maven-Artifacts-td30616.html
lazy val sparkVersion212 = "2.4.5"

// ---------------------------------------------------------------------------
resolvers += "Apache Repository" at "https://repository.apache.org/content/repositories/snapshots"

// ---------------------------------------------------------------------------
libraryDependencies +=
  (scalaBinaryVersion.value match {
    case "2.13" => "org.apache.spark" %% "spark-core" % sparkVersion213 % "provided" /* see 201217114746 */ withSources() withJavadoc()
    case _      => "org.apache.spark" %% "spark-core" % sparkVersion212 % "provided" /* see 201217114746 */ withSources() }) // not found: https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/2.4.5/spark-core_2.12-2.4.5-javadoc.jar

// ===========================================================================

