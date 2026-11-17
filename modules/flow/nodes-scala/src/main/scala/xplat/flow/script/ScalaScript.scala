package xplat.flow.script

import io.circe.Json

import java.net.URLClassLoader
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import scala.util.control.NonFatal

/** Code of a `script.scala` node, compiled once per text: the body of
  * {{{
  * def apply(state: Json, memory: Json, params: Json, log: String => Unit): Json
  * }}}
  * with `io.circe._`, `io.circe.syntax._`, `cats.syntax.all._` and [[ScalaScript.Api]] imported. It returns what `script.js` returns, as JSON:
  * `{values, port, memory}` (`out(...)` and `via(...)` build it), or `Json.Null` for nothing.
  *
  * The other files of the script go in the same class, before `apply`: what they define (`def`, `val`, `case class`, `object`,
  * `import`) the code can use, in Scala 2.13 and 3 alike. Their errors say the file and its line.
  */
object ScalaScript {

  /** What the compiled code is. */
  trait Fn {
    def apply(state: Json, memory: Json, params: Json, log: String => Unit): Json
  }

  /** Helpers that the code sees. */
  object Api {

    /** Goes on through `out` writing `values`. */
    def out(values: (String, Json)*): Json = Json.obj("values" -> Json.obj(values: _*))

    /** Goes on through `port` writing `values`. */
    def via(port: String, values: (String, Json)*): Json = Json.obj("values" -> Json.obj(values: _*), "port" -> Json.fromString(port))
  }

  private val Header: List[String] = List(
    "package xflowscript",
    "import io.circe._",
    "import io.circe.syntax._",
    "import cats.syntax.all._",
    "import xplat.flow.script.ScalaScript.Api._",
    "final class %s extends xplat.flow.script.ScalaScript.Fn {"
  )

  private val Apply: String = "  def apply(state: Json, memory: Json, params: Json, log: String => Unit): Json = {"

  /** Where the lines of the source come from: `file` is empty for the code itself.
    *
    * @param segments
    *   for each file and the code, its first line in the source, its number of lines and its name.
    */
  final case class Layout(segments: List[(Int, Int, Option[String])]) {

    /** The file and the line in it of a line of the source; none for the lines this object writes. */
    def locate(line: Int): Option[(Option[String], Int)] =
      segments.collectFirst { case (start, count, file) if line >= start && line < start + count => (file, line - start + 1) }

    /** `line N` for the code, `name line N` for a file. */
    def describe(line: Int): Option[String] = locate(line).map {
      case (None, l)       => s"line $l"
      case (Some(name), l) => s"$name line $l"
    }
  }

  /** The source of the class, its name and where its lines come from. */
  private[script] def source(code: String, files: List[ScriptSandbox.File] = Nil): (String, String, Layout) = {
    val name = "Script_" + hash(code :: files.flatMap(f => List(f.name, f.code)))
    val lines = scala.collection.mutable.ListBuffer.from(Header.updated(Header.size - 1, Header.last.format(name)))
    val segments = scala.collection.mutable.ListBuffer.empty[(Int, Int, Option[String])]
    def add(text: String, file: Option[String]): Unit = {
      val own = text.split("\n", -1).toList
      segments += ((lines.size + 1, own.size, file))
      lines ++= own
    }
    files.foreach(f => add(f.code, Some(f.name)))
    lines += Apply
    add(code, None)
    lines ++= List("  }", "}")
    (lines.mkString("\n"), name, Layout(segments.toList))
  }

  private def hash(parts: List[String]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    // Each part with its length first, so that two splits of the same text are not the same script.
    parts.foreach { p => digest.update(s"${p.length}:".getBytes(UTF_8)); digest.update(p.getBytes(UTF_8)) }
    digest.digest().take(12).map("%02x".format(_)).mkString
  }

  /** The jars and directories the code compiles against: the Scala library, circe, cats and this module. */
  private lazy val classpath: List[Path] = {
    val markers: List[Class[_]] = List(classOf[scala.Option[_]], classOf[Json], classOf[io.circe.numbers.BiggerDecimal], classOf[cats.Show[_]], classOf[cats.kernel.Eq[_]], classOf[Fn]) ++
      scala3Library.toList
    markers.flatMap(c => Option(c.getProtectionDomain.getCodeSource).map(cs => Paths.get(cs.getLocation.toURI))).distinct
  }

  /** On Scala 3, its own library (`scala3-library`) besides the 2.13 one. */
  private def scala3Library: Option[Class[_]] =
    try Some(Class.forName("scala.CanEqual", false, getClass.getClassLoader))
    catch { case _: ClassNotFoundException => None }

  /** The code compiled, and where its lines come from (to say where a failure is). */
  final case class Compiled(fn: Fn, layout: Layout)

  private val compiled = new ConcurrentHashMap[String, Either[String, Compiled]]()
  private val lock = new Object

  /** The code compiled, or why it does not compile (with the lines of the code as the user wrote it). Compiling takes seconds
    * the first time; the result is kept by text.
    */
  def compile(code: String, files: List[ScriptSandbox.File] = Nil): Either[String, Compiled] = {
    val (src, name, layout) = source(code, files)
    Option(compiled.get(name)).getOrElse {
      val result = lock.synchronized(Option(compiled.get(name)).getOrElse(build(src, name, layout)))
      compiled.put(name, result)
      result
    }
  }

  private def build(src: String, name: String, layout: Layout): Either[String, Compiled] = {
    val out = Files.createTempDirectory("flow-scala-")
    try {
      ScalaCompiler.compile(src, classpath, out) match {
        case Nil =>
          val loader = new URLClassLoader(Array(out.toUri.toURL), classOf[Fn].getClassLoader)
          Right(Compiled(loader.loadClass(s"xflowscript.$name").getDeclaredConstructor().newInstance().asInstanceOf[Fn], layout))
        case errors =>
          Left(errors.map { case (line, msg) => layout.describe(line).fold(msg)(where => s"$where: $msg") }.mkString("\n"))
      }
    } catch { case NonFatal(e) => Left(s"the compiler failed: ${e.getMessage}") }
  }
}
