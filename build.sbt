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
    Dependencies.Compile.cats,
    Dependencies.Compile.catsMtl,
    // Logging backend: the libraries only log through SLF4J, so the backend is Provided. It is available for
    // compiling, tests and `run`, but is not passed on to projects that depend on these modules.
    Dependencies.Compile.logbackClassic % Provided,
    Dependencies.Test.scalaTest,
    Dependencies.Test.catTest
  ),
  // Put Provided dependencies on the classpath of `run` and `runMain`, so the demo mains still log.
  Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
  Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated
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
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.doobieCore,
      Dependencies.Compile.doobieHikari
    )
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
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2
    )
  )

lazy val coreCoordination = (project in file("./modules/api/coordination"))
  .settings(
    name := "core-coordination",
    commonsSettings
  )

lazy val coreCluster = (project in file("./modules/api/cluster"))
  .aggregate(coreAPI, coreCoordination)
  .dependsOn(coreAPI, coreCoordination)
  .settings(
    name := "core-cluster",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2
    )
  )

lazy val coreSharding = (project in file("./modules/api/sharding"))
  .aggregate(coreAPI, coreCluster)
  .dependsOn(coreAPI, coreCluster)
  .settings(
    name := "core-sharding",
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

lazy val ioCoordination = (project in file("./modules/core/io-coordination"))
  .aggregate(coreCoordination)
  .dependsOn(coreCoordination)
  .settings(
    name := "io-coordination",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsEffects,
      Dependencies.Compile.catsFS2IO
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
  .aggregate(sqlitePersistence, optLibCore, coreSharding)
  .dependsOn(sqlitePersistence, optLibCore, coreSharding)
  .settings(
    name := "x-actor-platform",
    commonsSettings
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
