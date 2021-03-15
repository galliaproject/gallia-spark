// gallia-spark; TODO: t210309100048 - relies on symlink to gallia-core's project/*.scala files; no (reasonnable) sbt way? windows users will have to copy them instead?


// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    name    := "gallia-spark",
    version := "0.1.0")
  .settings(GalliaCommonSettings.mainSettings:_*)
  .dependsOn(RootProject(file("../gallia-core")))

// ===========================================================================
resolvers += "Apache Repository" at "https://repository.apache.org/content/repositories/snapshots"

// ---------------------------------------------------------------------------
lazy val sparkVersion213 = "3.2.0-20210212.011521-17" // per https://repository.apache.org/content/repositories/snapshots/org/apache/spark/spark-core_2.13/3.2.0-SNAPSHOT/, see http://apache-spark-developers-list.1001551.n3.nabble.com/FYI-Scala-2-13-Maven-Artifacts-td30616.html
lazy val sparkVersion212 = "2.4.5"

// ---------------------------------------------------------------------------
libraryDependencies +=
  (scalaBinaryVersion.value match {
    case "2.13" => "org.apache.spark" %% "spark-core" % sparkVersion213 % "provided" /* see 201217114746 */ withSources() withJavadoc()
    case _      => "org.apache.spark" %% "spark-core" % sparkVersion212 % "provided" /* see 201217114746 */ withSources() }) // not found: https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/2.4.5/spark-core_2.12-2.4.5-javadoc.jar

// ===========================================================================

