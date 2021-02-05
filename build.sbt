// gallia-spark

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    name         := "gallia-spark",
    version      := "0.1.0" )
  .dependsOn(RootProject(file("../gallia-core")))

// ===========================================================================
scalacOptions in Compile ++= Seq( // TODO: more + inherit
  "-Ywarn-value-discard",
  "-Ywarn-unused-import")

// ===========================================================================
//TODO: reuse dependsOn's
lazy val sparkVersion = "2.4.5"

// ===========================================================================
libraryDependencies +=
  "org.apache.spark" % "spark-core_2.12" % sparkVersion % "provided" /* see 201217114746 */ withSources() withJavadoc()

// ===========================================================================

