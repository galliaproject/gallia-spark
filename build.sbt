// gallia-spark

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    organizationName     := "Gallia Project",
    organization         := "io.github.galliaproject", // *must* match groupId for sonatype
    name                 := "gallia-spark",
    version              := GalliaCommonSettings.CurrentGalliaVersion,
    homepage             := Some(url("https://github.com/galliaproject/gallia-spark")),
    scmInfo              := Some(ScmInfo(
        browseUrl  = url("https://github.com/galliaproject/gallia-spark"),
        connection =     "scm:git@github.com:galliaproject/gallia-spark.git")),
    licenses             := Seq("Apache 2" -> url("https://github.com/galliaproject/gallia-spark/blob/master/LICENSE")),
    description          := "A Scala library for data manipulation" )
  .settings(GalliaCommonSettings.mainSettings:_*)

// ===========================================================================
lazy val sparkVersion212 = "3.3.0"
lazy val sparkVersion213 = "3.3.0"

// ---------------------------------------------------------------------------
libraryDependencies ++= Seq(
  "io.github.galliaproject" %% "gallia-core" % GalliaCommonSettings.CurrentGalliaVersion,
  (scalaBinaryVersion.value match {
    case "2.13" => "org.apache.spark" %% "spark-core" % sparkVersion213 % "provided" withSources() withJavadoc()
    case "2.12" => "org.apache.spark" %% "spark-core" % sparkVersion212 % "provided" withSources() withJavadoc() }))

// ===========================================================================
sonatypeRepository     := "https://s01.oss.sonatype.org/service/local"
sonatypeCredentialHost :=         "s01.oss.sonatype.org"
publishMavenStyle      := true
publishTo              := sonatypePublishToBundle.value

// ===========================================================================

