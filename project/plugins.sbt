// Publishing to Maven Central: version from git tags, signing and upload to the Sonatype Central Portal.
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.12.1")

// Scala.js: the actor core also runs in the browser and Node.
addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.20.1")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")

// Fat jar of the standalone flow server.
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

// `reStart`: runs the flow server in a fork that `~flowServer/reStart` restarts on every change.
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")
