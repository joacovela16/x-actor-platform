package xplat.flow.nodes

import cats.Functor
import cats.syntax.functor._
import io.circe.syntax._
import io.circe.{Decoder, Json}
import xplat.flow.entity.EntityCatalog
import xplat.flow.model._

/** The nodes of the business model: they build values of an entity from the state, check them and move them between their states.
  * They are pure, so they also run in simulations. Each run uses the entities as they are then (they can be edited while flows
  * run), through `entities`.
  *
  *   - `entity.build` `{entity, values}`: a value of the entity from `values`, an object whose texts can be references
  *     (`"${state.order.id}"`). Leaves through `out` with `value`, or through `invalid` with `errors`.
  *   - `entity.validate` `{entity, path}`: the value at `path` checked by the entity. Leaves through `valid` with `value` (with its
  *     defaults and computed fields), or through `invalid` with `errors`.
  *   - `entity.transition` `{entity, path, transition}`: the value at `path` moved by one of the transitions of the entity. Leaves
  *     through `out` with `value`, or through `rejected` with `errors` (a transition that does not start from its state, a guard
  *     that does not hold, a rule the new state breaks).
  *
  * `errors` is a list of `{path, rule, message}`. A node only writes what it declares.
  */
object EntityNodes {

  val Category: String = "Entities"

  val Build: String = "entity.build"
  val Validate: String = "entity.validate"
  val Transition: String = "entity.transition"

  val Valid: String = "valid"
  val Invalid: String = "invalid"
  val Rejected: String = "rejected"

  /** The type of `errors`. */
  val ErrorsType: DataType = DataType.ListOf(DataType.Obj(Map("path" -> DataType.Str, "rule" -> DataType.Str, "message" -> DataType.Str)))

  def nodes[F[_]: Functor](entities: F[EntityCatalog]): List[NodeType[F]] =
    List(new BuildNode(entities), new ValidateNode(entities), new TransitionNode(entities))

  lazy val catalog: NodeCatalog = new NodeCatalog(List(buildDescriptor, validateDescriptor, transitionDescriptor))

  // ---- configuration ----

  private val entityField =
    ConfigSchema.Field("entity", DataType.Str, description = Some("The entity of the value"), ui = Some(UiSpec.entity.copy(order = Some(0))))

  private val pathField =
    ConfigSchema.Field("path", DataType.Str, description = Some("Path of the value in the state, such as order"), ui = Some(UiSpec(order = Some(1))))

  private def entityOf(params: Json): Option[String] = params.hcursor.get[String]("entity").toOption.filter(_.nonEmpty)

  private def pathOf(params: Json): Option[StatePath] = params.hcursor.get[String]("path").toOption.flatMap(StatePath.parse(_).toOption)

  private def problemsOf(params: Json, needsPath: Boolean): List[String] =
    (if (entityOf(params).isEmpty) List("entity: the name of an entity is needed") else Nil) ++
      (if (needsPath) params.hcursor.get[String]("path").toOption.toList.flatMap(StatePath.parse(_).left.toOption.map(m => s"path: $m")) else Nil)

  /** Where the problems of a value at `path` go by default: next to it, as `<name>Errors`. */
  private def errorsBeside(path: StatePath): StatePath = StatePath(path.segments.init :+ s"${path.segments.last}Errors")

  /** `Order` → `order`. */
  private def nameFor(entity: String): String = entity.take(1).toLowerCase + entity.drop(1)

  private def outputs(entity: Option[String], value: StatePath): List[OutputDecl] =
    entity.toList.flatMap(e => List(OutputDecl("value", value, DataType.Entity(e)), OutputDecl("errors", errorsBeside(value), ErrorsType)))

  /** What the node answers: of `all`, what it declared (all of it when it is run on its own, declaring nothing). */
  private def answer(input: NodeInput, port: String, all: Map[String, Json]): NodeOutput = {
    val wanted = input.outputs.map(_.name).toSet
    NodeOutput(values = if (wanted.isEmpty) all else all.filter { case (k, _) => wanted(k) }, port = port)
  }

  private def errors(problems: List[DataType.Problem]): Json = problems.asJson

  // ---- entity.build ----

  val buildDescriptor: NodeDescriptor = NodeDescriptor(
    kind = Build,
    category = Category,
    label = "Build entity",
    description = "Builds a value of an entity from values and references of the state. Leaves through 'out', or through 'invalid' with the errors.",
    config = ConfigSchema(
      List(
        entityField,
        ConfigSchema.Field(
          "values",
          DataType.AnyJson,
          description = Some("The fields of the value: values or references such as ${state.order.id}"),
          ui = Some(UiSpec(widget = Some("json"), order = Some(1)))
        )
      )
    ),
    sideEffects = SideEffects.Pure,
    ports = _ => List(Port(Port.Out, writes = Some(List("value"))), Port(Invalid, writes = Some(List("errors")))),
    defaultOutputs = params => outputs(entityOf(params), StatePath(List(entityOf(params).map(nameFor).getOrElse("value")))),
    reads = params => params.hcursor.get[Json]("values").toOption.toList.flatMap(Template.reads).map(StateRead(_, DataType.AnyJson)),
    checkParams = params =>
      problemsOf(params, needsPath = false) ++
        params.hcursor.get[Json]("values").toOption.toList.flatMap { v =>
          (if (v.isObject) Nil else List("values: must be an object")) ++ Template.problems(v).map(m => s"values: $m")
        }
  )

  final class BuildNode[F[_]: Functor](entities: F[EntityCatalog]) extends NodeType[F] {
    val descriptor: NodeDescriptor = buildDescriptor

    def run(input: NodeInput): F[NodeOutput] = entities.map { catalog =>
      val c = input.params.hcursor
      (c.get[String]("entity"), c.getOrElse[Json]("values")(Json.obj())) match {
        case (Right(entity), Right(values)) =>
          Template.resolve(values, input) match {
            case Left(problem) => NodeOutput.failed(s"values: $problem")
            case Right(made) =>
              known(catalog, entity) {
                catalog.conform(entity, made) match {
                  case Right(value)   => answer(input, Port.Out, Map("value" -> value))
                  case Left(problems) => answer(input, Invalid, Map("errors" -> errors(problems)))
                }
              }
          }
        case (Left(e), _) => NodeOutput.failed(s"invalid configuration: ${e.message}")
        case (_, Left(e)) => NodeOutput.failed(s"invalid configuration: ${e.message}")
      }
    }
  }

  // ---- entity.validate ----

  val validateDescriptor: NodeDescriptor = NodeDescriptor(
    kind = Validate,
    category = Category,
    label = "Validate entity",
    description = "Checks a value of the state against an entity and fills its defaults and computed fields. Leaves through 'valid' or 'invalid'.",
    config = ConfigSchema(List(entityField, pathField)),
    sideEffects = SideEffects.Pure,
    ports = _ => List(Port(Valid, writes = Some(List("value"))), Port(Invalid, writes = Some(List("errors")))),
    defaultOutputs = params => pathOf(params).toList.flatMap(outputs(entityOf(params), _)),
    // What is checked can be anything: that is what the node is for.
    reads = params => pathOf(params).map(StateRead(_, DataType.AnyJson)).toList,
    checkParams = problemsOf(_, needsPath = true)
  )

  final class ValidateNode[F[_]: Functor](entities: F[EntityCatalog]) extends NodeType[F] {
    val descriptor: NodeDescriptor = validateDescriptor

    def run(input: NodeInput): F[NodeOutput] = entities.map { catalog =>
      withValue(input, catalog) { (entity, value) =>
        catalog.conform(entity, value) match {
          case Right(made)    => answer(input, Valid, Map("value" -> made))
          case Left(problems) => answer(input, Invalid, Map("errors" -> errors(problems)))
        }
      }
    }
  }

  // ---- entity.transition ----

  val transitionDescriptor: NodeDescriptor = NodeDescriptor(
    kind = Transition,
    category = Category,
    label = "Entity transition",
    description = "Moves a value of the state to another state of its entity through one of its transitions. Leaves through 'out', or through 'rejected' with the errors.",
    config = ConfigSchema(
      List(
        entityField,
        pathField,
        ConfigSchema.Field("transition", DataType.Str, description = Some("The name of the transition, such as approve"), ui = Some(UiSpec(order = Some(2))))
      )
    ),
    sideEffects = SideEffects.Pure,
    ports = _ => List(Port(Port.Out, writes = Some(List("value"))), Port(Rejected, writes = Some(List("errors")))),
    defaultOutputs = params => pathOf(params).toList.flatMap(outputs(entityOf(params), _)),
    // Only a value of the entity can be moved: one that comes from an input, a node or a check typed with it.
    reads = params => (for { p <- pathOf(params); e <- entityOf(params) } yield StateRead(p, DataType.Entity(e))).toList,
    checkParams = params =>
      problemsOf(params, needsPath = true) ++
        (if (params.hcursor.get[String]("transition").toOption.exists(_.nonEmpty)) Nil else List("transition: the name of a transition is needed"))
  )

  final class TransitionNode[F[_]: Functor](entities: F[EntityCatalog]) extends NodeType[F] {
    val descriptor: NodeDescriptor = transitionDescriptor

    def run(input: NodeInput): F[NodeOutput] = entities.map { catalog =>
      withValue(input, catalog) { (entity, value) =>
        input.params.hcursor.get[String]("transition") match {
          case Left(e) => NodeOutput.failed(s"invalid configuration: ${e.message}")
          case Right(name) =>
            catalog.transition(entity, value, name) match {
              case Right(moved)   => answer(input, Port.Out, Map("value" -> moved))
              case Left(problems) => answer(input, Rejected, Map("errors" -> errors(problems)))
            }
        }
      }
    }
  }

  /** An entity that does not exist is a mistake of the flow, not a value that is invalid: the node fails. */
  private def known(catalog: EntityCatalog, entity: String)(answer: => NodeOutput): NodeOutput =
    if (catalog.get(entity).isEmpty) NodeOutput.failed(s"the entity '$entity' does not exist") else answer

  /** The entity of the configuration and the value at its path, or the failure of the node. */
  private def withValue(input: NodeInput, catalog: EntityCatalog)(f: (String, Json) => NodeOutput): NodeOutput = {
    val c = input.params.hcursor
    (c.get[String]("entity"), c.get[StatePath]("path")(Decoder[StatePath])) match {
      case (Right(entity), Right(path)) =>
        known(catalog, entity) {
          path.get(input.state) match {
            case None        => NodeOutput.failed(s"'$path' is not in the state")
            case Some(value) => f(entity, value)
          }
        }
      case (Left(e), _) => NodeOutput.failed(s"invalid configuration: ${e.message}")
      case (_, Left(e)) => NodeOutput.failed(s"invalid configuration: ${e.message}")
    }
  }
}
