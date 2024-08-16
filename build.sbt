import Dependencies._

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "x-actor-platform"

val commonsSettings = Seq(
  libraryDependencies ++= Seq(
    // https://mvnrepository.com/artifact/com.lihaoyi/scalasql
    "com.lihaoyi" %% "scalasql" % "0.1.5",
    Dependencies.Compile.pprint,
    Dependencies.Compile.quicklens,
    Dependencies.Compile.catsSelfLogger,
    Dependencies.Compile.slf4j,
    Dependencies.Compile.logbackClassic,
    Dependencies.Compile.logbackCore,
    Dependencies.Compile.catsMtl,
    Dependencies.Test.scalaTest,
    Dependencies.Test.catTest
  )
)

lazy val coreAPI = (project in file("./modules/api/core"))
  .settings(
    name := "core-api",
    commonsSettings
  )

lazy val corePersistence = (project in file("./modules/api/persistence"))
  .aggregate(coreAPI)
  .dependsOn(coreAPI)
  .settings(
    name := "core-persistence",
    commonsSettings
  )

lazy val ioActor = (project in file("./modules/core/io"))
  .aggregate(coreAPI)
  .dependsOn(coreAPI)
  .settings(
    name := "io-actor",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.cats,
      Dependencies.Compile.catsFS2,
      Dependencies.Compile.catsEffects,
      Dependencies.Compile.catsMtl,
      Dependencies.Compile.catsLogger
    )
  )

lazy val root = (project in file("."))
  .settings(
    name := "x-actor-alive",
    commonsSettings
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
