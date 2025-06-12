import sbt.*

object Dependencies {

  object Versions {
    val scalaVersion: String = "2.13.16"

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
    val mysqlDriver: String = "8.0.33"
    val h2Driver: String = "2.3.232"

    val catsSelfLogger: String = "2.7.0"
    val slf4j: String = "2.0.9"

    val scalaTest: String = "3.2.19"
    val catTest: String = "1.6.0"
    val pprint: String = "0.9.0"
    val sourcecode: String = "0.4.2"

    val doobie: String = "1.0.0-RC9"

  }

  object Compile {

    val pprint = "com.lihaoyi" %% "pprint" % Versions.pprint
    val scalaReflect = "org.scala-lang" % "scala-reflect" % Versions.scalaVersion

    val cats = "org.typelevel" %% "cats-core" % Versions.cats
    val catsEffects = "org.typelevel" %% "cats-effect" % Versions.catsEffects
    val catsFS2 = "co.fs2" %% "fs2-core" % Versions.catsFS2
    val catsMtl = "org.typelevel" %% "cats-mtl" % Versions.catsMtl
    val catsLogger = "org.typelevel" %% "log4cats-core" % Versions.catsLogger
    val catsFree = "org.typelevel" %% "cats-free" % Versions.cats

    val catsSelfLogger = "org.typelevel" %% "log4cats-slf4j" % Versions.catsSelfLogger

    val quicklens = "com.softwaremill.quicklens" %% "quicklens" % Versions.quicklens
    val sourcecode = "com.lihaoyi" %% "sourcecode" % Versions.sourcecode

    // database
    val doobieCore = "org.tpolecat" %% "doobie-core" % Versions.doobie
    val doobieHikari = "org.tpolecat" %% "doobie-hikari" % Versions.doobie // HikariCP transactor.
    val sqliteDriver = "org.xerial" % "sqlite-jdbc" % Versions.sqliteDriver
    val mysqlDriver = "mysql" % "mysql-connector-java" % Versions.mysqlDriver
    val h2Driver = "com.h2database" % "h2" % Versions.h2Driver

    val chill = "com.twitter" %% "chill" % Versions.chill

    val circeCore = "io.circe" %% "circe-core" % Versions.circe
    val circeParser = "io.circe" %% "circe-parser" % Versions.circe
    val circeGeneric = "io.circe" %% "circe-generic" % Versions.circe

    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
    val logbackClassic = "ch.qos.logback" % "logback-classic" % Versions.logbackClassic
    val logbackCore = "ch.qos.logback" % "logback-core" % Versions.logbackClassic
  }

  object Test {

    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % "test"
    val catTest = "org.typelevel" %% "cats-effect-testing-scalatest" % Versions.catTest % "test"
  }

}
