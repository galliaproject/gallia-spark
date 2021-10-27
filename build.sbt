// gallia-spark

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    organizationName     := "Gallia Project",
    organization         := "io.github.galliaproject", // *must* match groupId for sonatype
    name                 := "gallia-spark",
    version              := "0.3.0",    
    homepage             := Some(url("https://github.com/galliaproject/gallia-spark")),
    scmInfo              := Some(ScmInfo(
        browseUrl  = url("https://github.com/galliaproject/gallia-spark"),
        connection =     "scm:git@github.com:galliaproject/gallia-spark.git")),
    licenses             := Seq("BSL 1.1" -> url("https://github.com/galliaproject/gallia-spark/blob/master/LICENSE")),
    description          := "A Scala library for data manipulation" )
  .settings(GalliaCommonSettings.mainSettings:_*)

// ===========================================================================
resolvers += "Apache Repository" at "https://repository.apache.org/content/repositories/snapshots"

// ---------------------------------------------------------------------------
lazy val galliaVersion   = "0.3.0"

lazy val sparkVersion213 = "3.2.0-20210212.011521-17" // per https://repository.apache.org/content/repositories/snapshots/org/apache/spark/spark-core_2.13/3.2.0-SNAPSHOT/, see http://apache-spark-developers-list.1001551.n3.nabble.com/FYI-Scala-2-13-Maven-Artifacts-td30616.html
lazy val sparkVersion212 = "2.4.5"

// ---------------------------------------------------------------------------
libraryDependencies ++= Seq(
  "io.github.galliaproject" %% "gallia-core" % galliaVersion,
  (scalaBinaryVersion.value match {
    case "2.13" => "org.apache.spark" %% "spark-core" % sparkVersion213 % "provided" withSources() withJavadoc()
    case _      => "org.apache.spark" %% "spark-core" % sparkVersion212 % "provided" withSources() })) // withJavadoc(): not found https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/2.4.5/spark-core_2.12-2.4.5-javadoc.jar

// ===========================================================================
sonatypeRepository     := "https://s01.oss.sonatype.org/service/local"
sonatypeCredentialHost :=         "s01.oss.sonatype.org"        
publishMavenStyle      := true
publishTo              := sonatypePublishToBundle.value

// ===========================================================================

