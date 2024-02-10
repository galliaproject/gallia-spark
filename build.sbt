// gallia-spark
//   trying to keep this to a mimimum
//   TODO: t210125110147 - investigate sbt alternatives, especially https://github.com/com-lihaoyi/mill

// ===========================================================================
ThisBuild / organizationName     := "Gallia Project"
ThisBuild / organization         := "io.github.galliaproject" // *must* match groupId for sonatype
ThisBuild / organizationHomepage := Some(url("https://github.com/galliaproject"))
ThisBuild / startYear            := Some(2021)
ThisBuild / version              := "0.6.1"
ThisBuild / description          := "A Scala library for data manipulation"
ThisBuild / homepage             := Some(url("https://github.com/galliaproject/gallia-spark"))
ThisBuild / licenses             := Seq("Apache 2" -> url("https://github.com/galliaproject/gallia-spark/blob/master/LICENSE"))
ThisBuild / developers           := List(Developer(
  id    = "anthony-cros",
  name  = "Anthony Cros",
  email = "contact.galliaproject@gmail.com",
  url   = url("https://github.com/anthony-cros")))
ThisBuild / scmInfo              := Some(ScmInfo(
  browseUrl  = url("https://github.com/galliaproject/gallia-spark"),
  connection =     "scm:git@github.com:galliaproject/gallia-spark.git"))

// ===========================================================================
lazy val root = (project in file("."))
  .settings(
    name   := "gallia-spark",
    target := baseDirectory.value / ".." / "bin" / "spark" /* TODO: t240103170440 - still leaves a reflect/target folder somehow */)
  .settings(GalliaCommonSettings.mainSettings:_*)

// ===========================================================================
lazy val sparkVersion212   = "3.5.0"
lazy val sparkVersion213   = "3.5.0"

lazy val uTestVersion      = "0.8.1"

// ---------------------------------------------------------------------------
ThisBuild / testFrameworks       += new TestFramework("utest.runner.Framework")
ThisBuild / libraryDependencies ++= Seq(
                     "io.github.galliaproject" %% "gallia-core" % version.value,
                     "com.lihaoyi"             %% "utest"       % uTestVersion % "test",
  (scalaBinaryVersion.value match {
    case "3"    => (("org.apache.spark" %% "spark-core" % sparkVersion213 % "provided").withSources().withJavadoc()).cross(CrossVersion.for3Use2_13)
    case "2.13" =>   "org.apache.spark" %% "spark-core" % sparkVersion213 % "provided"  withSources() withJavadoc()
    case "2.12" =>   "org.apache.spark" %% "spark-core" % sparkVersion212 % "provided"  withSources() withJavadoc() }))

// ===========================================================================
sonatypeRepository     := "https://s01.oss.sonatype.org/service/local"
sonatypeCredentialHost :=         "s01.oss.sonatype.org"
publishMavenStyle      := true
publishTo              := sonatypePublishToBundle.value

// ===========================================================================

