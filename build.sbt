Global / onChangedBuildSource := ReloadOnSourceChanges

// One forked test module at a time: those are the database and broker modules, each with its own container, so small
// machines such as CI runners never run several containers at once.
Global / concurrentRestrictions += Tags.limit(Tags.ForkedTestGroup, 1)

ThisBuild / scalaVersion := Dependencies.Versions.scalaVersion
ThisBuild / crossScalaVersions := Seq(Dependencies.Versions.scalaVersion, Dependencies.Versions.scala3Version)
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

/** What every module needs, on the JVM and on Scala.js. */
val sharedSettings = Seq(
  libraryDependencies ++= Seq(
    Dependencies.Compile.pprint.value,
    Dependencies.Compile.quicklens.value,
    Dependencies.Compile.cats.value,
    Dependencies.Compile.catsEffects.value,
    Dependencies.Compile.catsMtl.value,
    Dependencies.Test.scalaTest.value,
    Dependencies.Test.catTest.value
  )
)

/** `jvm/src` and `js/src` next to the shared `src`: the `Pure` layout has no directory per platform of its own. */
def platformSources(platform: String) =
  Seq(Compile -> "main", Test -> "test").map { case (config, name) =>
    config / unmanagedSourceDirectories += baseDirectory.value.getParentFile / platform / "src" / name / "scala"
  }

/** JVM only: SLF4J logging and `run`. Scala.js has neither. */
val jvmSettings = Seq(
  libraryDependencies ++= Seq(
    Dependencies.Compile.catsSelfLogger,
    // Logging backend: the libraries only log through SLF4J, so the backend is Provided. It is available for
    // compiling, tests and `run`, but is not passed on to projects that depend on these modules.
    Dependencies.Compile.logbackClassic % Provided
  ),
  // Put Provided dependencies on the classpath of `run` and `runMain`, so the demo mains still log.
  Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
  Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated
)

val commonsSettings = sharedSettings ++ jvmSettings

// Modules that also run on Scala.js are `crossProject`s with the `Pure` layout: sources in `src` are shared, and
// `jvm/src` and `js/src` hold what is per platform.

lazy val coreAPI = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/api/core"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .settings(name := "xplat-core-api")
  // `UUID.randomUUID` on Scala.js needs a source of randomness.
  // It is not published for Scala 3; it only has Java classes, so the 2.13 artifact does.
  .jsSettings(
    libraryDependencies += {
      if (scalaBinaryVersion.value == "3") "org.scala-js" % "scalajs-java-securerandom_sjs1_2.13" % "1.0.0"
      else "org.scala-js" %%% "scalajs-java-securerandom" % "1.0.0"
    }
  )

//lazy val macros  = (project in file("./modules/api/macros"))
//  .settings(
//  )
lazy val corePersistence = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/api/persistence"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .dependsOn(coreAPI)
  .settings(
    name := "xplat-core-persistence",
    libraryDependencies += Dependencies.Compile.catsFS2.value
  )

lazy val coreCoordination = (project in file("./modules/api/coordination"))
  .settings(
    name := "xplat-core-coordination",
    commonsSettings
  )

lazy val coreCluster = (project in file("./modules/api/cluster"))
  .aggregate(coreAPI.jvm, coreCoordination)
  .dependsOn(coreAPI.jvm, coreCoordination)
  .settings(
    name := "xplat-core-cluster",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2.value
    )
  )

lazy val coreSharding = (project in file("./modules/api/sharding"))
  .aggregate(coreAPI.jvm, coreCluster)
  .dependsOn(coreAPI.jvm, coreCluster)
  .settings(
    name := "xplat-core-sharding",
    commonsSettings
  )

lazy val ioActor = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/core/io"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .dependsOn(coreAPI, corePersistence)
  .settings(
    name := "xplat-io-actor",
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2.value,
      Dependencies.Compile.catsLogger.value
    )
  )
  .jsSettings(libraryDependencies += Dependencies.Compile.catsLoggerConsole.value)

lazy val ioCoordination = (project in file("./modules/core/io-coordination"))
  .aggregate(coreAPI.jvm, coreCoordination)
  .dependsOn(coreAPI.jvm, coreCoordination)
  .settings(
    name := "xplat-io-coordination",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsEffects.value,
      Dependencies.Compile.catsFS2IO
    )
  )

lazy val ioCluster = (project in file("./modules/core/io-cluster"))
  .aggregate(ioActor.jvm, coreCluster, ioCoordination)
  .dependsOn(ioActor.jvm, coreCluster, ioCoordination)
  .settings(
    name := "xplat-io-cluster",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsEffects.value,
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
  .dependsOn(ioActor.jvm, ioCoordination)
  .settings(
    name := "xplat-persistence-testkit",
    commonsSettings,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % Dependencies.Versions.scalaTest,
      "org.typelevel" %% "cats-effect-testing-scalatest" % Dependencies.Versions.catTest
    )
  )

lazy val jdbcPersistence = (project in file("./modules/extensions/persistence/jdbc"))
  .dependsOn(corePersistence.jvm, ioCoordination)
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
  .dependsOn(corePersistence.jvm, ioCoordination, persistenceTestkit % Test)
  .settings(
    name := "xplat-persistence-mongodb",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.mongoDriver,
    Test / fork := true,
    Test / parallelExecution := false
  )

lazy val coreConnectors = (project in file("./modules/api/connectors"))
  .dependsOn(coreAPI.jvm, ioActor.jvm % Test)
  .settings(
    name := "xplat-core-connectors",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.catsEffects.value
  )

/** Settings of a connector module: tests run the shared contract against a broker in a container. */
def connectorModule(id: String, moduleName: String, deps: Seq[ModuleID]) =
  Project(id, file(s"./modules/extensions/connectors/$moduleName"))
    .dependsOn(coreConnectors % "compile->compile;test->test", ioActor.jvm % Test, persistenceTestkit % Test)
    .settings(
      name := s"xplat-$moduleName",
      commonsSettings,
      libraryDependencies ++= deps,
      Test / fork := true,
      Test / parallelExecution := false
    )

lazy val kafkaConnector = connectorModule("kafkaConnector", "kafka", Seq(Dependencies.Compile.fs2Kafka))
lazy val activemqConnector = connectorModule(
  "activemqConnector",
  "activemq",
  Seq(Dependencies.Compile.jakartaJms, Dependencies.Compile.artemisClient % Optional, Dependencies.Compile.activemqClassicClient % Optional)
)
lazy val redisConnector = connectorModule("redisConnector", "redis", Seq(Dependencies.Compile.redis4cats))

lazy val circeCodec = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/extensions/codec/circe"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .dependsOn(coreAPI)
  .settings(
    name := "xplat-circe",
    libraryDependencies ++= Seq(Dependencies.Compile.circeCore.value, Dependencies.Compile.circeParser.value, Dependencies.Compile.circeGeneric.value % Test)
  )

lazy val di = (project in file("./modules/core/di"))
  .settings(
    name := "xplat-di",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.catsEffects.value
  )

/** Runnable examples, the same as in the README. Not published. */
lazy val examples = (project in file("./modules/examples"))
  .dependsOn(ioSharding, sqlitePersistence, postgresPersistence, di, kafkaConnector, activemqConnector, redisConnector, circeCodec.jvm)
  .settings(
    name := "xplat-examples",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.artemisClient,
    publish / skip := true
  )

// Every published module is listed, so `publish` and `test` on the root reach all of them.
lazy val root = (project in file("."))
  .aggregate(
    coreAPI.jvm,
    coreAPI.js,
    corePersistence.jvm,
    corePersistence.js,
    coreCoordination,
    coreCluster,
    coreSharding,
    ioActor.jvm,
    ioActor.js,
    ioCoordination,
    ioCluster,
    ioSharding,
    persistenceTestkit,
    jdbcPersistence,
    sqlitePersistence,
    postgresPersistence,
    mariadbPersistence,
    mongoPersistence,
    di,
    coreConnectors,
    circeCodec.jvm,
    circeCodec.js,
    kafkaConnector,
    activemqConnector,
    redisConnector,
    examples
  )
  .dependsOn(coreSharding, ioSharding, sqlitePersistence, postgresPersistence, mariadbPersistence, mongoPersistence)
  .settings(
    name := "x-actor-platform",
    commonsSettings,
    // The root only aggregates the modules; it is not a library.
    publish / skip := true
  )

