import sbt._
import Keys._

lazy val sentrySparkVersion = "0.0.1-alpha05"

lazy val sparkVersion = "2.4.5"
lazy val sentryVersion = "5.0.0"

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala211, scala212)

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "io.sentry",
  version := sentrySparkVersion,
  crossScalaVersions := supportedScalaVersions,
  scalacOptions ++= Seq("-target:jvm-1.8",
                        "-deprecation",
                        "-feature",
                        "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalafmtOnCompile := true
)

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/getsentry/sentry-spark")),
  licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  bintrayRepository := "sentry-spark",
  scmInfo := Some(
    ScmInfo(
      browseUrl = url("https://github.com/getsentry/sentry-spark"),
      connection = "scm:git:ssh://ithub.com:getsentry/sentry-spark.git",
      devConnection = Some("scm:git:ssh://github.com:getsentry/sentry-spark.git")
    )
  ),
  bintrayOrganization := Some("getsentry"),
  developers := List(
    Developer("abhiprasad", "Abhijeet Prasad", "aprasad@sentry.io", url("https://github.com/abhiprasad"))
  )
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    name := "sentry-spark",
    description := "Sentry Integration for Apache Spark",
    run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    parallelExecution in Test := false,
    fork in Test := false,
    javaOptions in Test ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "io.sentry" % "sentry" % sentryVersion,
      // Testing
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    )
  )
