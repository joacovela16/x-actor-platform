package xplat.flow.script

import io.circe.Json
import io.circe.parser.parse
import org.graalvm.polyglot._
import org.graalvm.polyglot.io.IOAccess
import org.graalvm.polyglot.proxy.ProxyExecutable

import java.io.OutputStream
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** Runs the JavaScript that users write in the editor, with nothing of the machine within reach.
  *
  * What a script can do is only what the language does by itself: it gets its inputs as plain JSON values, and returns a value.
  * Every context is built with all host access off: no Java classes (`Java.type`, `Packages`, `java`), no files, no threads, no
  * native code, no environment variables, no `load`, `print`, `console` or `Polyglot`. `require`, `process` and `fetch` do not exist
  * in this engine. Inputs are frozen and the code is strict, so a script cannot change what it is given: assigning to `state` fails.
  *
  * Limits: time (the context is cancelled when it passes), statements executed, the size of the result and the number of log lines.
  * The engine has no limit of memory on a JDK that is not GraalVM Enterprise: a script that fills the heap in the time it has is
  * not stopped by anything but its timeout, so keep it short.
  *
  * The engine runs in interpreter mode on a normal JDK, which is slow to start and several times slower than on GraalVM. One
  * [[Session]] compiles the code once and can be called for many elements.
  */
object ScriptSandbox {

  /** @param timeout
    *   the longest a call may take.
    * @param statements
    *   the most statements one call may run.
    * @param maxResultBytes
    *   the largest result, as JSON.
    * @param maxLogLines
    *   the log lines kept of one call.
    */
  final case class Limits(timeout: FiniteDuration = 5.seconds, statements: Long = 20000000L, maxResultBytes: Int = 1 << 20, maxLogLines: Int = 50) {
    require(timeout > Duration.Zero, "the timeout must be positive")
    def withTimeout(t: FiniteDuration): Limits = copy(timeout = t min timeout)
  }

  /** Another file of a script: it runs before the code, so what it declares at its top level (`function`, `const`, `let`, `class`)
    * the code can use. The files run in the order given, each with its own lines.
    */
  final case class File(name: String, code: String)

  /** Why a script did not give a result.
    *
    * @param line
    *   the line of the script, counting from 1, when it is known.
    * @param file
    *   the [[File]] where it is, when it is not the code itself.
    */
  final case class Error(kind: Error.Kind, message: String, line: Option[Int] = None, file: Option[String] = None) {
    def render: String = (file, line) match {
      case (Some(f), Some(l)) => s"$f line $l: $message"
      case (Some(f), None)    => s"$f: $message"
      case (None, Some(l))    => s"line $l: $message"
      case (None, None)       => message
    }
  }

  object Error {
    sealed trait Kind
    case object Syntax extends Kind
    case object Runtime extends Kind
    case object Timeout extends Kind
    case object LimitExceeded extends Kind
    case object BadResult extends Kind
  }

  /** What a call returned: `value` is empty when the script returned `undefined`. */
  final case class Result(value: Option[Json], logs: List[String])

  private val SourceName = "script.js"

  private val Helpers =
    """(function () {
      |  function freeze(o) { if (o && typeof o === 'object' && !Object.isFrozen(o)) { Object.freeze(o); Object.keys(o).forEach(function (k) { freeze(o[k]); }); } return o; }
      |  return {
      |    frozen: function (s) { return freeze(JSON.parse(s)); },
      |    wrap: function (v) { return JSON.stringify({ d: v === undefined, v: v === undefined ? null : v }); }
      |  };
      |})()""".stripMargin

  private val NoOutput: OutputStream = OutputStream.nullOutputStream()

  // One engine for all the contexts: it keeps the code that was already compiled. It starts the first time it is needed.
  private lazy val engine: Engine =
    Engine.newBuilder("js").option("engine.WarnInterpreterOnly", "false").out(NoOutput).err(NoOutput).logHandler(NoOutput).build()

  private lazy val watchdog: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    def newThread(r: Runnable): Thread = { val t = new Thread(r, "flow-script-watchdog"); t.setDaemon(true); t }
  })

  private def newContext(limits: Limits): Context =
    Context
      .newBuilder("js")
      .engine(engine)
      .allowHostAccess(HostAccess.NONE)
      .allowHostClassLookup(_ => false)
      .allowIO(IOAccess.NONE)
      .allowCreateThread(false)
      .allowNativeAccess(false)
      .allowCreateProcess(false)
      .allowEnvironmentAccess(EnvironmentAccess.NONE)
      .allowPolyglotAccess(PolyglotAccess.NONE)
      // Some options that turn features off are marked experimental. Only this code sets options: a script cannot.
      .allowExperimentalOptions(true)
      .option("js.java-package-globals", "false")
      .option("js.load", "false")
      .option("js.polyglot-builtin", "false")
      .option("js.print", "false")
      .option("js.console", "false")
      .resourceLimits(ResourceLimits.newBuilder().statementLimit(limits.statements, null).build())
      .build()

  /** Compiles `code`, the body of a function that takes `args` and returns a value, and keeps a context to run it in. Close it when
    * done. It is not for several threads at a time.
    */
  final class Session private[ScriptSandbox] (code: String, args: List[String], val limits: Limits, files: List[File]) {

    private var context: Context = _
    private var function: Value = _
    private var helpers: Value = _
    @volatile private var cancelled = false

    // The whole prologue is on the first line, so the lines of the errors are the lines of the code.
    private val source: Source =
      Source.newBuilder("js", s"(function(${args.mkString(",")},log){\"use strict\";$code\n})", SourceName).buildLiteral()

    private val fileSources: List[Source] = files.map(f => Source.newBuilder("js", f.code, FilePrefix + f.name).buildLiteral())

    private[ScriptSandbox] def open(): Option[Error] = {
      close()
      try {
        val ctx = newContext(limits)
        context = ctx
        helpers = ctx.eval("js", Helpers)
        fileSources.foreach(ctx.eval)
        function = ctx.eval(source)
        None
      } catch {
        case e: PolyglotException => close(); Some(error(e, timedOut = false))
        case NonFatal(e)          => close(); Some(Error(Error.Runtime, Option(e.getMessage).getOrElse(e.toString)))
      }
    }

    /** Runs it with these values (one for each argument, as JSON), within the limits of the session. */
    def call(values: List[Json], timeout: FiniteDuration = limits.timeout): Either[Error, Result] =
      (if (context == null) open() else None) match {
        case Some(failure) => Left(failure)
        case None          => run(values, timeout)
      }

    private def run(values: List[Json], timeout: FiniteDuration): Either[Error, Result] = {
      val logs = scala.collection.mutable.ListBuffer.empty[String]
      val log = new ProxyExecutable {
        def execute(arguments: Value*): AnyRef = {
          if (logs.size < limits.maxLogLines) logs += arguments.map(a => if (a.isString) a.asString else a.toString).mkString(" ")
          null
        }
      }
      cancelled = false
      val ctx = context
      val timer = watchdog.schedule(new Runnable { def run(): Unit = { cancelled = true; try ctx.close(true) catch { case NonFatal(_) => () } } }, timeout.toMillis, TimeUnit.MILLISECONDS)
      try {
        ctx.resetLimits()
        val frozen = values.map(v => helpers.getMember("frozen").execute(v.noSpaces))
        val result = function.execute((frozen :+ log.asInstanceOf[AnyRef]).map(_.asInstanceOf[AnyRef]): _*)
        val text = helpers.getMember("wrap").execute(result).asString
        if (text.length > limits.maxResultBytes) Left(Error(Error.BadResult, s"the result is larger than ${limits.maxResultBytes} bytes"))
        else
          parse(text).toOption.map(_.hcursor) match {
            case Some(c) if c.get[Boolean]("d").contains(true) => Right(Result(None, logs.toList))
            case Some(c) =>
              c.downField("v").focus match {
                case Some(v) => Right(Result(Some(v), logs.toList))
                case None    => Left(Error(Error.BadResult, "the result is not a JSON value"))
              }
            case None => Left(Error(Error.BadResult, "the result is not a JSON value"))
          }
      } catch {
        case e: PolyglotException =>
          val failure = error(e, cancelled)
          // A context that was cancelled is closed: the next call builds a new one.
          if (cancelled) { context = null }
          Left(failure)
        case NonFatal(e) => Left(Error(Error.Runtime, Option(e.getMessage).getOrElse(e.toString)))
      } finally {
        timer.cancel(false)
        // The watchdog may have gone off just after the call ended: the context is then closed too.
        if (cancelled) context = null
      }
    }

    def close(): Unit = {
      val ctx = context
      context = null
      function = null
      helpers = null
      if (ctx != null) try ctx.close(true) catch { case NonFatal(_) => () }
    }
  }

  /** The names of the sources of the files start with this, so that no file is taken for the code. */
  private val FilePrefix = "file:"

  private def error(e: PolyglotException, timedOut: Boolean): Error = {
    def ours(l: SourceSection): Boolean = l != null && l.isAvailable && (l.getSource.getName == SourceName || l.getSource.getName.startsWith(FilePrefix))
    def place(l: SourceSection): (Option[String], Int) =
      (Some(l.getSource.getName).filter(_.startsWith(FilePrefix)).map(_.stripPrefix(FilePrefix)), l.getStartLine)
    // An error that is thrown has no location of its own, but its first frame in the script does.
    val frames = e.getPolyglotStackTrace.iterator()
    val fromFrames = Iterator.continually(frames).takeWhile(_.hasNext).map(_.next()).collectFirst { case f if f.isGuestFrame && ours(f.getSourceLocation) => place(f.getSourceLocation) }
    val (file, line) = Option(e.getSourceLocation).filter(ours).map(place).orElse(fromFrames) match {
      case Some((f, l)) => (f, Some(l))
      case None         => (None, None)
    }
    if (timedOut) Error(Error.Timeout, "the script took too long and was stopped")
    else if (e.isResourceExhausted) Error(Error.LimitExceeded, "the script ran more statements than allowed and was stopped")
    else if (e.isCancelled || e.isInterrupted) Error(Error.Timeout, "the script took too long and was stopped")
    else if (e.isSyntaxError) Error(Error.Syntax, Option(e.getMessage).getOrElse("syntax error").replaceFirst("^SyntaxError: ", "").replaceFirst("^file:", ""), line, file)
    else if (e.isGuestException) Error(Error.Runtime, Option(e.getMessage).getOrElse("error"), line, file)
    else Error(Error.Runtime, s"the script engine failed: ${Option(e.getMessage).getOrElse(e.toString)}")
  }

  /** Compiles the body of a function with the given arguments, or says what is wrong with it. */
  def open(code: String, args: List[String], limits: Limits = Limits(), files: List[File] = Nil): Either[Error, Session] = {
    val session = new Session(code, args, limits, files)
    session.open().toLeft(session)
  }

  /** Whether `code` compiles, as the body of a function with these arguments. */
  def check(code: String, args: List[String], files: List[File] = Nil): Option[Error] =
    open(code, args, Limits(timeout = 2.seconds), files) match {
      case Left(error)    => Some(error)
      case Right(session) => session.close(); None
    }
}
