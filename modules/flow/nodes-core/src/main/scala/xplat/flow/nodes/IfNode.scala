package xplat.flow.nodes

import cats.Applicative
import cats.syntax.applicative._
import io.circe.{Decoder, DecodingFailure, Json}
import xplat.flow.model._

/** Branches on a boolean in the state, or in the memory of the node: goes on through `then` when it is true and through
  * `else` when it is false. It fails if the variable is missing or is not a boolean, so a mistake is not hidden by
  * taking the `else`.
  */
final class IfNode[F[_]: Applicative] extends NodeType[F] {

  val descriptor: NodeDescriptor = IfNode.descriptor

  def run(input: NodeInput): F[NodeOutput] = IfNode.evaluate(input).pure[F]
}

object IfNode {

  val Kind: String = "core.if"
  val Then: String = "then"
  val Else: String = "else"

  /** Where the variable is. `Memory` is the memory of this node, not of any other. */
  sealed trait Source
  object Source {
    case object State extends Source
    case object Memory extends Source

    val names: Map[String, Source] = Map("state" -> State, "memory" -> Memory)

    implicit val decoder: Decoder[Source] = Decoder.instance { c =>
      c.as[String].flatMap(s => names.get(s).toRight(DecodingFailure(s"unknown source '$s'", c.history)))
    }
  }

  /** @param negate
    *   takes `then` when the variable is false.
    */
  final case class Params(path: StatePath, source: Source = Source.State, negate: Boolean = false)

  object Params {
    implicit val decoder: Decoder[Params] = Decoder.instance { c =>
      for {
        path <- c.get[StatePath]("path")
        source <- c.getOrElse[Source]("source")(Source.State)
        negate <- c.getOrElse[Boolean]("negate")(false)
      } yield Params(path, source, negate)
    }
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Flow control",
    label = "If",
    description = "Goes on through 'then' when a boolean of the state is true and through 'else' when it is false. It fails if the value is missing or is not a boolean.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("path", DataType.Str, description = Some("Path of the boolean, such as order.approved")),
        ConfigSchema.Field(
          "source",
          DataType.Str,
          required = false,
          default = Some(Json.fromString("state")),
          oneOf = Source.names.keys.toList.sorted.map(Json.fromString),
          description = Some("Where the variable is: the global state or the memory of this node")
        ),
        ConfigSchema.Field("negate", DataType.Bool, required = false, default = Some(Json.False))
      )
    ),
    sideEffects = SideEffects.Pure,
    ports = _ => List(Port(Then), Port(Else)),
    reads = params => params.as[Params].toOption.filter(_.source == Source.State).map(p => StateRead(p.path, DataType.Bool)).toList,
    checkParams = params => params.as[Params].left.toOption.map(e => s"path: ${e.message}").toList
  )

  /** The decision, without any effect. */
  def evaluate(input: NodeInput): NodeOutput =
    input.params.as[Params] match {
      case Left(error) => NodeOutput.failed(s"invalid configuration: ${error.message}")
      case Right(params) =>
        val (root, where) = params.source match {
          case Source.State  => (input.state, "the state")
          case Source.Memory => (input.memory, "the memory")
        }
        params.path.get(root) match {
          case None => NodeOutput.failed(s"'${params.path}' is not in $where")
          case Some(value) =>
            value.asBoolean match {
              case None          => NodeOutput.failed(s"'${params.path}' in $where is not a boolean: ${value.noSpaces}")
              case Some(boolean) => NodeOutput.via(if (boolean != params.negate) Then else Else)
            }
        }
    }
}
