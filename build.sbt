// gallia-spark

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    name         := "gallia-spark",
    version      := "0.1.0",
    scalaVersion := "2.13.4" /* TODO: inherit from core */)
  .dependsOn(RootProject(file("../gallia-core")))

// ===========================================================================
// TODO: more + inherit from core
scalacOptions in Compile ++=
  Seq("-Ywarn-value-discard") ++ 
  (scalaBinaryVersion.value match {
    case "2.13" => Seq("-Ywarn-unused:imports")
    case _      => Seq("-Ywarn-unused-import" ) })

// ===========================================================================
lazy val sparkVersion = "3.2.0-20210212.011521-17" //was "2.4.5"; per https://repository.apache.org/content/repositories/snapshots/org/apache/spark/spark-core_2.13/3.2.0-SNAPSHOT/, see http://apache-spark-developers-list.1001551.n3.nabble.com/FYI-Scala-2-13-Maven-Artifacts-td30616.html

// ---------------------------------------------------------------------------
resolvers += "Apache Repository" at "https://repository.apache.org/content/repositories/snapshots"

// ---------------------------------------------------------------------------
libraryDependencies +=
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" /* see 201217114746 */ withSources() withJavadoc()

// ===========================================================================

