Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / scalaVersion := Dependencies.Versions.scalaVersion
// The version comes from git tags (sbt-ci-release / sbt-dynver): tag v0.1.0 publishes 0.1.0, untagged commits publish
// snapshots.
ThisBuild / organization := "io.github.joacovela16"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / organizationName := "x-actor-platform"
ThisBuild / homepage := Some(url("https://github.com/joacovela16/x-actor-platform"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / developers := List(
  Developer("joacovela16", "Joaquín Velázquez Camacho", "joaquinvelazquezcamacho@gmail.com", url("https://github.com/joacovela16"))
)
ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/joacovela16/x-actor-platform"), "scm:git@github.com:joacovela16/x-actor-platform.git")
)
ThisBuild / description := "Typed actors for cats-effect: persistence, cluster membership, singletons, sharding and reliable delivery."

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

lazy val coreAPI = (project in file("./modules/api/core"))
  .settings(
    name := "xplat-core-api",
    commonsSettings
  )

//lazy val macros  = (project in file("./modules/api/macros"))
//  .settings(
//  )
lazy val corePersistence = (project in file("./modules/api/persistence"))
  .aggregate(coreAPI)
  .dependsOn(coreAPI)
  .settings(
    name := "xplat-core-persistence",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2
    )
  )

lazy val coreCoordination = (project in file("./modules/api/coordination"))
  .settings(
    name := "xplat-core-coordination",
    commonsSettings
  )

lazy val coreCluster = (project in file("./modules/api/cluster"))
  .aggregate(coreAPI, coreCoordination)
  .dependsOn(coreAPI, coreCoordination)
  .settings(
    name := "xplat-core-cluster",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2
    )
  )

lazy val coreSharding = (project in file("./modules/api/sharding"))
  .aggregate(coreAPI, coreCluster)
  .dependsOn(coreAPI, coreCluster)
  .settings(
    name := "xplat-core-sharding",
    commonsSettings
  )

lazy val ioActor = (project in file("./modules/core/io"))
  .aggregate(coreAPI, corePersistence)
  .dependsOn(coreAPI, corePersistence)
  .settings(
    name := "xplat-io-actor",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2
    )
  )

lazy val ioCoordination = (project in file("./modules/core/io-coordination"))
  .aggregate(coreAPI, coreCoordination)
  .dependsOn(coreAPI, coreCoordination)
  .settings(
    name := "xplat-io-coordination",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsEffects,
      Dependencies.Compile.catsFS2IO
    )
  )

lazy val ioCluster = (project in file("./modules/core/io-cluster"))
  .aggregate(ioActor, coreCluster, ioCoordination)
  .dependsOn(ioActor, coreCluster, ioCoordination)
  .settings(
    name := "xplat-io-cluster",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsEffects,
      Dependencies.Compile.catsFS2IO
    )
  )

lazy val ioSharding = (project in file("./modules/core/io-sharding"))
  .aggregate(ioCluster, coreSharding)
  .dependsOn(ioCluster % "compile->compile;test->test", coreSharding)
  .settings(
    name := "xplat-io-sharding",
    commonsSettings
  )

lazy val persistenceTestkit = (project in file("./modules/extensions/persistence/testkit"))
  .dependsOn(ioActor, ioCoordination)
  .settings(
    name := "xplat-persistence-testkit",
    commonsSettings,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % Dependencies.Versions.scalaTest,
      "org.typelevel" %% "cats-effect-testing-scalatest" % Dependencies.Versions.catTest
    )
  )

lazy val jdbcPersistence = (project in file("./modules/extensions/persistence/jdbc"))
  .dependsOn(corePersistence, ioCoordination)
  .settings(
    name := "xplat-persistence-jdbc",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.doobieCore,
      Dependencies.Compile.doobieHikari
    )
  )

/** Settings of a database module: tests run against the shared contracts in the testkit. */
def databaseModule(id: String, moduleName: String, driver: ModuleID) =
  Project(id, file(s"./modules/extensions/persistence/$moduleName"))
    .dependsOn(jdbcPersistence, persistenceTestkit % Test)
    .settings(
      name := s"xplat-persistence-$moduleName",
      commonsSettings,
      libraryDependencies += driver,
      Test / fork := true,
      Test / parallelExecution := false
    )

lazy val sqlitePersistence = databaseModule("sqlitePersistence", "sqlite", Dependencies.Compile.sqliteDriver)
lazy val postgresPersistence = databaseModule("postgresPersistence", "postgres", Dependencies.Compile.postgresDriver)
lazy val mariadbPersistence = databaseModule("mariadbPersistence", "mariadb", Dependencies.Compile.mariadbDriver)

lazy val mongoPersistence = (project in file("./modules/extensions/persistence/mongodb"))
  .dependsOn(corePersistence, ioCoordination, persistenceTestkit % Test)
  .settings(
    name := "xplat-persistence-mongodb",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.mongoDriver,
    Test / fork := true,
    Test / parallelExecution := false
  )

/** Runnable examples, the same as in the README. Not published. */
lazy val examples = (project in file("./modules/examples"))
  .dependsOn(ioSharding, sqlitePersistence, postgresPersistence)
  .settings(
    name := "xplat-examples",
    commonsSettings,
    publish / skip := true
  )

// Every published module is listed, so `publish` and `test` on the root reach all of them.
lazy val root = (project in file("."))
  .aggregate(
    coreAPI,
    corePersistence,
    coreCoordination,
    coreCluster,
    coreSharding,
    ioActor,
    ioCoordination,
    ioCluster,
    ioSharding,
    persistenceTestkit,
    jdbcPersistence,
    sqlitePersistence,
    postgresPersistence,
    mariadbPersistence,
    mongoPersistence,
    examples
  )
  .dependsOn(coreSharding, ioSharding, sqlitePersistence, postgresPersistence, mariadbPersistence, mongoPersistence)
  .settings(
    name := "x-actor-platform",
    commonsSettings,
    // The root only aggregates the modules; it is not a library.
    publish / skip := true
  )

