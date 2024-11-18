Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / scalaVersion := Dependencies.Versions.scalaVersion
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "x-actor-platform"

val commonsSettings = Seq(
  libraryDependencies ++= Seq(
    Dependencies.Compile.pprint,
    Dependencies.Compile.quicklens,
    Dependencies.Compile.catsSelfLogger,
    Dependencies.Compile.logbackClassic,
    Dependencies.Compile.cats,
    Dependencies.Compile.catsMtl,
    Dependencies.Test.scalaTest,
    Dependencies.Test.catTest
  )
)

lazy val exprLibCore = (project in file("./modules/library/expr/core"))
  .settings(
    name := "expr-lib-core",
    libraryDependencies ++= Seq(
      Dependencies.Compile.scalaReflect,
      Dependencies.Compile.catsFS2,
      Dependencies.Compile.catsEffects
    ),
    commonsSettings
  )

lazy val sqlLibCore = (project in file("./modules/library/expr/sql"))
  .aggregate(exprLibCore)
  .dependsOn(exprLibCore)
  .settings(
    name := "sql-lib-core",
    commonsSettings
  )

lazy val coreAPI = (project in file("./modules/api/core"))
  .settings(
    name := "core-api",
    commonsSettings
  )

//lazy val macros  = (project in file("./modules/api/macros"))
//  .settings(
//  )
lazy val corePersistence = (project in file("./modules/api/persistence"))
  .aggregate(coreAPI)
  .dependsOn(coreAPI)
  .settings(
    name := "core-persistence",
    commonsSettings
  )

lazy val ioActor = (project in file("./modules/core/io"))
  .aggregate(coreAPI, corePersistence)
  .dependsOn(coreAPI, corePersistence)
  .settings(
    name := "io-actor",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2
    )
  )

lazy val sqlitePersistence = (project in file("./modules/extensions/persistence/sqlite"))
  .aggregate(coreAPI, corePersistence, sqlLibCore, ioActor)
  .dependsOn(coreAPI, corePersistence, sqlLibCore, ioActor)
  .settings(
    name := "xplat-sqlite-persistence",
    libraryDependencies ++= Seq(
      Dependencies.Compile.sqliteDriver
    ),
    commonsSettings
  )

lazy val optLibCore = (project in file("./modules/library/optimizer"))
  .enablePlugins(spray.boilerplate.BoilerplatePlugin)
  .aggregate(coreAPI, ioActor)
  .dependsOn(coreAPI, ioActor)
  .settings(
    name := "optimizer",
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2,
      Dependencies.Compile.cats,
      Dependencies.Compile.catsEffects,
      Dependencies.Compile.sourcecode,
      Dependencies.Compile.catsFree,
      "org.scalanlp" %% "breeze" % "2.1.0"
    ),
    commonsSettings
  )

lazy val root = (project in file("."))
  .aggregate(sqlitePersistence, optLibCore)
  .dependsOn(sqlitePersistence, optLibCore)
  .settings(
    name := "x-actor-platform",
    commonsSettings
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
