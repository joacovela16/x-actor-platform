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

lazy val flowModel = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/flow/model"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .settings(
    name := "xplat-flow-model",
    libraryDependencies ++= Seq(Dependencies.Compile.circeCore.value, Dependencies.Compile.circeParser.value % Test)
  )

/** The entities of the business model (`EntityDef`): their fields, rules, computed fields and states, the checks of a value against
  * them, and a DSL to write them in Scala. Flows use them as types (`DataType.Entity`, `DataType.Ref`).
  */
lazy val flowEntity = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/flow/entity"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .dependsOn(flowModel)
  .settings(
    name := "xplat-flow-entity",
    libraryDependencies += Dependencies.Compile.circeParser.value % Test
  )

/** The contract of a kind of node (`NodeType`, connections, the pool of clients) and of a plugin (`FlowPlugin`, its
  * manifest). Plugins compile against this as `Provided`, so it stays small and versioned (`FlowPlugin.ApiVersion`).
  */
lazy val flowNodesApi = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/flow/nodes-api"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .dependsOn(flowModel, flowEntity)
  .settings(
    name := "xplat-flow-nodes-api",
    libraryDependencies += Dependencies.Compile.circeParser.value % Test
  )

/** The nodes that come with the flow module (`If`...). */
lazy val flowNodesCore = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .withoutSuffixFor(JVMPlatform)
  .in(file("./modules/flow/nodes-core"))
  .settings(sharedSettings)
  .jvmSettings(jvmSettings, platformSources("jvm"))
  .jsSettings(platformSources("js"))
  .dependsOn(flowNodesApi)
  .settings(
    name := "xplat-flow-nodes-core",
    libraryDependencies += Dependencies.Compile.circeParser.value % Test
  )

/** Runs flows: instances as persistent actors, the store of definitions and the simulator. */
lazy val flowEngine = (project in file("./modules/flow/engine"))
  .dependsOn(flowNodesCore.jvm, ioActor.jvm, corePersistence.jvm)
  .settings(
    name := "xplat-flow-engine",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.catsFS2.value,
      Dependencies.Compile.circeGeneric.value,
      Dependencies.Compile.circeParser.value
    )
  )

/** The routes of the flow module: API, hooks of the triggers and the static files of the UI. Written over any `F`. */
lazy val flowHttp = (project in file("./modules/flow/http"))
  .dependsOn(flowEngine)
  .settings(
    name := "xplat-flow-http",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.http4sDsl,
      Dependencies.Compile.http4sCirce,
      Dependencies.Compile.circeParser.value
    )
  )

/** Nodes that talk to message brokers (`queue.publish`) and the runner of `queue` triggers. Optional: the server adds them. */
lazy val flowNodesQueue = (project in file("./modules/flow/nodes-queue"))
  .dependsOn(flowHttp, kafkaConnector, activemqConnector, redisConnector)
  .settings(
    name := "xplat-flow-nodes-queue",
    commonsSettings,
    // The JMS clients are optional in the connector, so the module that opens connections to them brings them.
    libraryDependencies ++= Seq(Dependencies.Compile.artemisClient, Dependencies.Compile.activemqClassicClient)
  )

/** The nodes that process streams (sources, operators and sinks) and the node that runs a whole stream segment. Optional: the
  * server adds them. They use the broker connectors of `flowNodesQueue` for queue sources and publishing sinks.
  */
lazy val flowNodesStream = (project in file("./modules/flow/nodes-stream"))
  .dependsOn(flowNodesQueue)
  .settings(
    name := "xplat-flow-nodes-stream",
    commonsSettings,
    // TestControl runs the tests of windows and throttling with a clock that jumps.
    libraryDependencies += Dependencies.Compile.catsEffectTestkit % Test,
    // On a fork, a segment that hangs in a test does not take the sbt server with it.
    Test / fork := true
  )

/** The dynamic node `script.js` and the stream operator `stream.script`: JavaScript that the user writes in the editor, run in a
  * strict sandbox (GraalJS). Optional: the server adds it.
  */
lazy val flowNodesScript = (project in file("./modules/flow/nodes-script"))
  .dependsOn(flowNodesStream % "compile->compile;test->test")
  .settings(
    name := "xplat-flow-nodes-script",
    commonsSettings,
    libraryDependencies ++= Seq(Dependencies.Compile.graalPolyglot, Dependencies.Compile.graalJs),
    // The JS engine is slow to start and runs on a fork so its own settings are the ones that apply.
    Test / fork := true
  )

/** `script.scala`: a node whose code is Scala written in the editor, compiled in the server with the compiler of the server's own
  * Scala version (2.13 or 3). It is not sandboxed, so the server only offers it with `--scala-nodes true`.
  */
lazy val flowNodesScala = (project in file("./modules/flow/nodes-scala"))
  .dependsOn(flowNodesScript)
  .settings(
    name := "xplat-flow-nodes-scala",
    commonsSettings,
    libraryDependencies += {
      if (scalaBinaryVersion.value == "3") "org.scala-lang" %% "scala3-compiler" % scalaVersion.value
      else "org.scala-lang" % "scala-compiler" % scalaVersion.value
    },
    // The compiler needs the classpath as files: a fork has it in `java.class.path`, like the server.
    Test / fork := true
  )

/** The nodes that read and write relational databases (`db.query`, `db.insert`, `db.update`, `db.delete`, `db.execute`) over JDBC
  * and doobie, with a connection pool for each named connection (`postgres`, `mariadb`, `sqlite`). Optional: the server adds them.
  */
lazy val flowNodesDb = (project in file("./modules/flow/nodes-db"))
  .dependsOn(flowHttp, flowNodesStream)
  .settings(
    name := "xplat-flow-nodes-db",
    commonsSettings,
    libraryDependencies ++= Seq(
      Dependencies.Compile.doobieCore,
      Dependencies.Compile.doobieHikari,
      Dependencies.Compile.sqliteDriver,
      Dependencies.Compile.postgresDriver,
      Dependencies.Compile.mariadbDriver
    )
  )

/** The nodes that read and write MongoDB (`mongo.find`, `mongo.aggregate`, `mongo.insert`, `mongo.update`, `mongo.delete`) over the
  * sync driver, with a client for each named `mongodb` connection, and the change streams that `queue` triggers on a `mongodb`
  * connection read. Optional: the server adds them.
  */
lazy val flowNodesMongo = (project in file("./modules/flow/nodes-mongo"))
  .dependsOn(flowHttp, flowNodesStream, coreConnectors, persistenceTestkit % Test)
  .settings(
    name := "xplat-flow-nodes-mongo",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.mongoDriver,
    Test / fork := true
  )

/** The node that calls HTTP services (`http.request`) over the HTTP client of the JDK, with the `http` connections. Optional: the
  * server adds it.
  */
lazy val flowNodesHttp = (project in file("./modules/flow/nodes-http"))
  .dependsOn(flowHttp)
  .settings(
    name := "xplat-flow-nodes-http",
    commonsSettings
  )

/** The AI agent node (`ai.agent`) and the connections it uses (`claude`, `openai`, `codex`), over the HTTP client of http4s. Optional:
  * the server adds it.
  */
lazy val flowNodesAi = (project in file("./modules/flow/nodes-ai"))
  .dependsOn(flowHttp)
  .settings(
    name := "xplat-flow-nodes-ai",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.http4sEmberClient
  )

/** Loads the plugins of a directory (`jvm` jars in a class loader of their own) and names their kinds `<plugin>.<kind>@<major>`. */
lazy val flowPlugins = (project in file("./modules/flow/plugins"))
  // nodes-core for `Template`: the loader resolves the references of the params before a plugin's node runs.
  .dependsOn(flowNodesCore.jvm)
  .settings(
    name := "xplat-flow-plugins",
    commonsSettings,
    libraryDependencies += Dependencies.Compile.circeParser.value,
    // The tests load the example plugin from its jar, as the server would: it is not on their classpath.
    Test / resourceGenerators += Def.task {
      val jar = (LocalProject("flowPluginExample") / Compile / packageBin).value
      val file = (Test / resourceManaged).value / "example-plugin.path"
      IO.write(file, jar.getAbsolutePath)
      // And the Python example with its SDK, for the `process` transport.
      val py = (Test / resourceManaged).value / "python-plugin.path"
      IO.write(py, (baseDirectory.value / "..").getCanonicalPath)
      Seq(file, py)
    }.taskValue
  )

/** An example plugin, with a node in Scala and one in Java. It is not published: the tests of `flowPlugins` load its jar. */
lazy val flowPluginExample = (project in file("./modules/flow/plugin-example"))
  .dependsOn(flowNodesApi.jvm % Provided, flowNodesTestkit % Test)
  .settings(
    name := "xplat-flow-plugin-example",
    sharedSettings,
    publish / skip := true,
    // The contract suite loads the plugin's own jar, as the server would.
    Test / resourceGenerators += Def.task {
      val jar = (Compile / packageBin).value
      val file = (Test / resourceManaged).value / "plugin.jar.path"
      IO.write(file, jar.getAbsolutePath)
      val manifest = (Test / resourceManaged).value / "plugin.manifest.path"
      IO.write(manifest, (baseDirectory.value / "flow-plugin.json").getAbsolutePath)
      Seq(file, manifest)
    }.taskValue
  )

/** For whoever writes a plugin: `PluginHarness` loads it as the server does (its own class loader, kinds renamed, references
  * resolved, or its process started) and `PluginContract` is the suite every plugin should pass. A plugin adds it `% Test`.
  */
lazy val flowNodesTestkit = (project in file("./modules/flow/nodes-testkit"))
  .dependsOn(flowPlugins)
  .settings(
    name := "xplat-flow-nodes-testkit",
    commonsSettings,
    libraryDependencies += "org.scalatest" %% "scalatest" % Dependencies.Versions.scalaTest,
    // Its own tests run the Python example through the contract.
    Test / resourceGenerators += Def.task {
      val file = (Test / resourceManaged).value / "flow-dir.path"
      IO.write(file, (baseDirectory.value / "..").getCanonicalPath)
      Seq(file)
    }.taskValue
  )

/** The UI is built with bun or npm (`modules/flow/ui`) and goes into the classpath under `/public`. It is skipped, with a warning,
  * when neither bun nor npm is there or when `-Dflow.ui.skip=true` / `FLOW_UI_SKIP=true` is set, so the other modules never depend on it.
  */
val flowUiBuild = taskKey[Seq[File]]("Builds the UI of the flow module with bun/npm and lists the files for the classpath under /public")

/** The standalone flow server: `AppIO`, the only place that names `IO`. `flowServer/assembly` builds an executable jar. */
lazy val flowServer = (project in file("./modules/flow/server"))
  .dependsOn(flowHttp, flowNodesQueue, flowNodesStream, flowNodesScript, flowNodesScala, flowNodesDb, flowNodesMongo, flowNodesHttp, flowNodesAi, flowPlugins, sqlitePersistence, mongoPersistence)
  .settings(
    name := "xplat-flow-server",
    commonsSettings,
    // The server is an application: the logging backend goes in the jar.
    libraryDependencies ++= Seq(Dependencies.Compile.http4sEmberServer, Dependencies.Compile.logbackClassic) ++ Dependencies.Compile.openTelemetry,
    publish / skip := true,
    Compile / mainClass := Some("xplat.flow.server.AppIO"),
    assembly / mainClass := Some("xplat.flow.server.AppIO"),
    assembly / assemblyJarName := "flow-server.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*)                        => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(n => n.endsWith(".SF") || n.endsWith(".DSA") || n.endsWith(".RSA")) => MergeStrategy.discard
      case PathList("META-INF", "versions", _*)                        => MergeStrategy.first
      // GraalJS (the script nodes) reads its version files and the Truffle runtime libraries from these.
      case PathList("META-INF", "graalvm", _*)                         => MergeStrategy.first
      case PathList("META-INF", "resources", _*)                       => MergeStrategy.first
      case PathList("META-INF", _*)                                    => MergeStrategy.discard
      case "module-info.class"                                         => MergeStrategy.discard
      case x if x.endsWith("reference.conf")                           => MergeStrategy.concat
      case _                                                           => MergeStrategy.first
    },
    flowUiBuild := {
      import scala.sys.process._
      val log = streams.value.log
      val cacheDir = streams.value.cacheDirectory / "flow-ui"
      val ui = (ThisBuild / baseDirectory).value / "modules" / "flow" / "ui"
      val out = (Compile / resourceManaged).value / "public"
      val skip = sys.props.get("flow.ui.skip").orElse(sys.env.get("FLOW_UI_SKIP")).contains("true")
      def available(tool: String) = scala.util.Try(Process(Seq(tool, "--version")).!(ProcessLogger(_ => ())) == 0).getOrElse(false)
      // bun if installed (faster), npm otherwise; `FLOW_UI_PM=npm|bun` forces one.
      val pm = sys.env.get("FLOW_UI_PM").filter(available).orElse(Seq("bun", "npm").find(available))
      if (skip) {
        log.warn("flow ui: skipped (flow.ui.skip); the server will answer 503 on '/'")
        Nil
      } else if (pm.isEmpty) {
        log.warn("flow ui: neither bun nor npm is available, so the UI is not built; the server will answer 503 on '/'")
        Nil
      } else {
        val sources = (ui / "src" ** "*").get ++ Seq("index.html", "package.json", "package-lock.json", "bun.lock", "vite.config.ts", "tsconfig.json").map(ui / _).filter(_.exists)
        val cached = FileFunction.cached(cacheDir, FilesInfo.lastModified, FilesInfo.exists) { _ =>
          def run(cmd: String*): Unit = {
            val code = Process(cmd, ui).!(ProcessLogger(l => log.info(s"[ui] $l"), l => log.warn(s"[ui] $l")))
            if (code != 0) sys.error(s"flow ui: '${cmd.mkString(" ")}' failed with exit code $code")
          }
          val tool = pm.get
          if (!(ui / "node_modules").exists) {
            if (tool == "bun") run("bun", "install") else run("npm", "ci", "--no-audit", "--no-fund")
          }
          run(tool, "run", "build")
          IO.delete(out)
          IO.copyDirectory(ui / "dist", out)
          (out ** "*").get.filter(_.isFile).toSet
        }
        cached(sources.filter(_.isFile).toSet).toSeq
      }
    },
    Compile / resourceGenerators += flowUiBuild.taskValue,
    // `~flowServer/reStart` also restarts when the UI changes (flowUiBuild rebuilds it on the way).
    watchSources ++= {
      val ui = (ThisBuild / baseDirectory).value / "modules" / "flow" / "ui"
      Seq(WatchSource(ui / "src")) ++
        Seq("index.html", "package.json", "vite.config.ts", "tsconfig.json").map(n => WatchSource(ui / n))
    }
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
    flowModel.jvm,
    flowModel.js,
    flowEntity.jvm,
    flowEntity.js,
    flowNodesApi.jvm,
    flowNodesApi.js,
    flowNodesCore.jvm,
    flowNodesCore.js,
    flowEngine,
    flowHttp,
    flowNodesQueue,
    flowNodesStream,
    flowNodesScript,
    flowNodesDb,
    flowNodesMongo,
    flowNodesHttp,
    flowNodesAi,
    flowPlugins,
    flowNodesScala,
    flowNodesTestkit,
    flowPluginExample,
    flowServer,
    examples
  )
  .dependsOn(coreSharding, ioSharding, sqlitePersistence, postgresPersistence, mariadbPersistence, mongoPersistence)
  .settings(
    name := "x-actor-platform",
    commonsSettings,
    // The root only aggregates the modules; it is not a library.
    publish / skip := true
  )

