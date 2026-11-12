import sbt.*
import org.portablescala.sbtplatformdeps.PlatformDepsPlugin.autoImport.*

object Dependencies {

  object Versions {
    val scalaVersion: String = "2.13.18"
    val scala3Version: String = "3.3.6"

    val quicklens: String = "1.9.12"
    val chill: String = "0.10.0"

    val circe: String = "0.14.13"
    val config: String = "1.4.2"
    val ficusversion: String = "1.5.2"

    val logbackClassic: String = "1.5.18"
    val scalaLogging: String = "3.9.5"

    val catsEffects: String = "3.6.1"
    val cats: String = "2.13.0"
    val catsFS2: String = "3.12.0"
    val catsLogger: String = "2.7.0"
    val catsMtl: String = "1.5.0"

    val quill: String = "4.8.0"
    val sqliteDriver: String = "3.49.1.0"
    val postgresDriver: String = "42.7.4"
    val mariadbDriver: String = "3.4.1"
    val mongoDriver: String = "5.1.4"
    val mysqlDriver: String = "8.0.33"
    val h2Driver: String = "2.3.232"

    val catsSelfLogger: String = "2.7.0"
    val slf4j: String = "2.0.9"

    val scalaTest: String = "3.2.19"
    val catTest: String = "1.6.0"
    val pprint: String = "0.9.0"

    val doobie: String = "1.0.0-RC9"

    val fs2Kafka: String = "3.9.1"
    val redis4cats: String = "2.0.6"
    val jakartaJms: String = "3.1.0"
    val artemis: String = "2.40.0"
    val activemqClassic: String = "6.1.6"

  }

  object Compile {

    val pprint = Def.setting("com.lihaoyi" %%% "pprint" % Versions.pprint)

    val cats = Def.setting("org.typelevel" %%% "cats-core" % Versions.cats)
    val catsEffects = Def.setting("org.typelevel" %%% "cats-effect" % Versions.catsEffects)
    val catsFS2 = Def.setting("co.fs2" %%% "fs2-core" % Versions.catsFS2)
    val catsFS2IO = "co.fs2" %% "fs2-io" % Versions.catsFS2
    val catsMtl = Def.setting("org.typelevel" %%% "cats-mtl" % Versions.catsMtl)
    val catsLogger = Def.setting("org.typelevel" %%% "log4cats-core" % Versions.catsLogger)

    val catsLoggerConsole = Def.setting("org.typelevel" %%% "log4cats-js-console" % Versions.catsLogger)

    val catsSelfLogger = "org.typelevel" %% "log4cats-slf4j" % Versions.catsSelfLogger

    val quicklens = Def.setting("com.softwaremill.quicklens" %%% "quicklens" % Versions.quicklens)

    // database
    val doobieCore = "org.tpolecat" %% "doobie-core" % Versions.doobie
    val doobieHikari = "org.tpolecat" %% "doobie-hikari" % Versions.doobie // HikariCP transactor.
    val sqliteDriver = "org.xerial" % "sqlite-jdbc" % Versions.sqliteDriver
    val postgresDriver = "org.postgresql" % "postgresql" % Versions.postgresDriver
    val mariadbDriver = "org.mariadb.jdbc" % "mariadb-java-client" % Versions.mariadbDriver
    val mongoDriver = "org.mongodb" % "mongodb-driver-sync" % Versions.mongoDriver
    val mysqlDriver = "mysql" % "mysql-connector-java" % Versions.mysqlDriver
    val h2Driver = "com.h2database" % "h2" % Versions.h2Driver

    // connectors
    val fs2Kafka = "com.github.fd4s" %% "fs2-kafka" % Versions.fs2Kafka
    val redis4cats = "dev.profunktor" %% "redis4cats-effects" % Versions.redis4cats
    val jakartaJms = "jakarta.jms" % "jakarta.jms-api" % Versions.jakartaJms
    val artemisClient = "org.apache.activemq" % "artemis-jakarta-client" % Versions.artemis
    val activemqClassicClient = "org.apache.activemq" % "activemq-client" % Versions.activemqClassic

    val chill = "com.twitter" %% "chill" % Versions.chill

    val circeCore = Def.setting("io.circe" %%% "circe-core" % Versions.circe)
    val circeParser = Def.setting("io.circe" %%% "circe-parser" % Versions.circe)
    val circeGeneric = Def.setting("io.circe" %%% "circe-generic" % Versions.circe)

    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
    val logbackClassic = "ch.qos.logback" % "logback-classic" % Versions.logbackClassic
    val logbackCore = "ch.qos.logback" % "logback-core" % Versions.logbackClassic
  }

  object Test {

    val scalaTest = Def.setting("org.scalatest" %%% "scalatest" % Versions.scalaTest % "test")
    val catTest = Def.setting("org.typelevel" %%% "cats-effect-testing-scalatest" % Versions.catTest % "test")
  }

}
