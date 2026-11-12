// Publishing to Maven Central: version from git tags, signing and upload to the Sonatype Central Portal.
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.12.1")

// Scala.js: the actor core also runs in the browser and Node.
addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.20.1")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")
