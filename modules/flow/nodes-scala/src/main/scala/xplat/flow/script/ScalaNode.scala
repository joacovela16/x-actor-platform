package xplat.flow.script

import cats.effect.Sync
import cats.syntax.all._
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.NodeType

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

/** `script.scala`: like `script.js`, with Scala:
  * {{{
  * val items = state.hcursor.downField("order").downField("items").as[List[Json]].getOrElse(Nil)
  * log(s"${items.size} items")
  * out("count" -> items.size.asJson)
  * }}}
  * The code is the body of a function of `(state, memory, params, log)` that returns `{values, port, memory}` as JSON (`out(...)`,
  * `via(port, ...)`), compiled the first time it runs (see [[ScalaScript]]); a mistake fails the node with the line.
  *
  * It is NOT sandboxed: the code runs in the server's JVM and can do anything the server can. A time limit stops waiting for it but
  * cannot stop the thread.
  */
final class ScalaNode[F[_]](implicit F: Sync[F]) extends NodeType[F] {

  val descriptor: NodeDescriptor = ScalaNode.descriptor

  def run(input: NodeInput): F[NodeOutput] =
    ScriptNode.params(input.params) match {
      case Left(problem) => NodeOutput.failed(problem).pure[F]
      case Right(p) =>
        F.blocking {
          ScalaScript.compile(p.code, p.files) match {
            case Left(errors) => NodeOutput.failed(s"the code does not compile:\n$errors")
            case Right(ScalaScript.Compiled(fn, layout)) =>
              val logs = ListBuffer.empty[String]
              try {
                val value = fn(input.state, input.memory, p.args, line => logs.synchronized(logs += line))
                ScriptNode.output(p, ScriptSandbox.Result(Option(value), logs.toList))
              } catch {
                case NonFatal(e) =>
                  val where = e.getStackTrace.find(_.getClassName.startsWith("xflowscript.")).flatMap(f => layout.describe(f.getLineNumber)).fold("")(w => s" ($w)")
                  NodeOutput.failed(s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}$where")
              }
          }
        }
    }
}

object ScalaNode {

  val Kind: String = "script.scala"

  val descriptor: NodeDescriptor = ScriptNode.descriptor.copy(
    kind = Kind,
    label = "Script (Scala)",
    config = ConfigSchema(ScriptNode.descriptor.config.fields.map {
      case f if f.name == "code" =>
        f.copy(
          description = Some("Scala: the body of a function of (state, memory, params, log) that returns Json. out(\"x\" -> 1.asJson) writes x; via(port, ...) chooses the port."),
          ui = Some(UiSpec.script("scala", Template, "files"))
        )
      case f if f.name == "files" => ScriptNode.filesField("scala", "scala")
      case f => f
    }),
    // The syntax is checked when the node runs: compiling takes seconds.
    checkParams = json =>
      ScriptNode.params(json) match {
        case Left(problem) => List(problem)
        case Right(p) =>
          (if (p.ports.contains(Port.Error)) List("'error' is a port of every node already; leave it out of ports") else Nil) ++
            ScriptNode.fileProblems(p.files, "scala")
      }
  )

  /** The code a new node starts with: it writes the outputs of [[ScriptNode.TemplateOutputs]]. */
  lazy val Template: String =
    """// state, memory and params are io.circe.Json; log(text) writes a line.
      |// The files of the tabs are compiled with this code: what they define can be used here.
      |val items = state.hcursor.downField("order").get[List[Json]]("items").getOrElse(Nil)
      |val total = items.map { i =>
      |  val c = i.hcursor
      |  c.get[Double]("price").getOrElse(0d) * c.get[Double]("qty").getOrElse(0d)
      |}.sum
      |log(s"total $total")
      |
      |// out(...) goes on through out; via("port", ...) through another of its ports.
      |out("total" -> total.asJson, "big" -> (total > 1000).asJson)
      |""".stripMargin

  def nodes[F[_]: Sync]: List[NodeType[F]] = List(new ScalaNode[F])

  /** The descriptor only, for validation without running. */
  val catalog: NodeCatalog = new NodeCatalog(List(descriptor))
}
