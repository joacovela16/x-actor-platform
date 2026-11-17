package xplat.flow.script

import scala.reflect.internal.util.BatchSourceFile
import scala.tools.nsc.reporters.StoreReporter
import scala.tools.nsc.{Global, Settings}

import java.io.File
import java.nio.file.Path

/** The Scala 2.13 compiler, for a server built with 2.13. */
private[script] object ScalaCompiler {

  /** Compiles `source` into `out`; the errors, as (line, message), when it does not compile. */
  def compile(source: String, classpath: List[Path], out: Path): List[(Int, String)] = {
    val settings = new Settings()
    settings.usejavacp.value = false
    settings.classpath.value = classpath.mkString(File.pathSeparator)
    settings.outdir.value = out.toString
    settings.nowarn.value = true
    val reporter = new StoreReporter(settings)
    val global = new Global(settings, reporter)
    val run = new global.Run
    run.compileSources(List(new BatchSourceFile("script.scala", source)))
    reporter.infos.toList.filter(_.severity == reporter.ERROR).map(i => (if (i.pos.isDefined) i.pos.line else 0, i.msg))
  }
}
