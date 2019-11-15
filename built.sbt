import sbt._
import Keys._

val sparkVersion = "2.4.4"
val sentryVersion = "1.7.28"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "io.sentry",
  version := "0.0.1-alpha",
  scalaVersion := "2.11.12",
  scalacOptions ++= Seq("-target:jvm-1.8",
                        "-deprecation",
                        "-feature",
                        "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalafmtOnCompile := true
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
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
