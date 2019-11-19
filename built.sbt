import sbt._
import Keys._

val sparkVersion = "2.4.4"
val sentryVersion = "1.7.28"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "io.sentry",
  version := "0.0.1-alpha01",
  scalaVersion := "2.11.12",
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
      connection = "https://github.com/getsentry/sentry-spark",
      devConnection = Some("https://github.com/getsentry/sentry-spark")
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
    fork in Test := true,
    javaOptions in Test ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "io.sentry" % "sentry" % sentryVersion,
      // Testing
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    )
  )
