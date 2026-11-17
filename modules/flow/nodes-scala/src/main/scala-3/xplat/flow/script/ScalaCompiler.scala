package xplat.flow.script

import dotty.tools.dotc.Main
import dotty.tools.dotc.interfaces.{Diagnostic, SimpleReporter}

import java.io.File
import java.nio.file.{Files, Path}
import scala.collection.mutable.ListBuffer

/** The Scala 3 compiler, for a server built with Scala 3. */
private[script] object ScalaCompiler {

  /** Compiles `source` into `out`; the errors, as (line, message), when it does not compile. */
  def compile(source: String, classpath: List[Path], out: Path): List[(Int, String)] = {
    val file = Files.createTempFile("script", ".scala")
    try {
      Files.writeString(file, source)
      val errors = ListBuffer.empty[(Int, String)]
      val reporter: SimpleReporter = (d: Diagnostic) =>
        if (d.level == Diagnostic.ERROR) {
          val pos = d.position()
          errors += ((if (pos.isPresent) pos.get.line + 1 else 0, d.message))
        }
      Main.process(Array("-classpath", classpath.mkString(File.pathSeparator), "-d", out.toString, "-nowarn", file.toString), reporter, null)
      errors.toList
    } finally Files.deleteIfExists(file)
  }
}
