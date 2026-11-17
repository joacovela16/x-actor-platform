package xplat.flow.entity

import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json, JsonObject}
import xplat.flow.model.{DataType, Expr}

/** An entity of the business model: its fields (with nested values and references to other entities), the rules a value of it
  * keeps, the fields computed from the others and the states it goes through. It is plain data, stored and exchanged as JSON;
  * the DSL ([[dsl]]) writes the same thing from code.
  *
  * Flows use it as a type: [[DataType.Entity]] is a whole value of it, [[DataType.Ref]] the value of its key.
  *
  * @param kind
  *   a `root` has an identity (`key`) and can be referenced; a `value` has none and is only held inside others.
  * @param key
  *   the field that identifies a value of a root, text or a number.
  * @param rules
  *   conditions on several fields that every value keeps.
  * @param states
  *   the field that holds the state of a value and the transitions between states.
  * @param revision
  *   the revision of the content, which grows with every change that is saved.
  * @param version
  *   the version of the format.
  */
final case class EntityDef(
    name: String,
    fields: List[EntityDef.Field],
    kind: EntityDef.Kind = EntityDef.Kind.Root,
    key: Option[String] = None,
    rules: List[EntityDef.Rule] = Nil,
    states: Option[EntityDef.StateMachine] = None,
    label: Option[String] = None,
    description: Option[String] = None,
    revision: Int = 1,
    version: Int = EntityDef.CurrentVersion
) {

  def field(name: String): Option[EntityDef.Field] = fields.find(_.name == name)

  /** The type of each field, which is what flows see of a value of it. */
  def fieldTypes: Map[String, DataType] = fields.map(f => f.name -> f.tpe).toMap

  /** The type of its key, which is the type of a reference to it; only a root has one. */
  def keyType: Option[DataType] = if (kind == EntityDef.Kind.Root) key.flatMap(field).map(_.tpe) else None
}

object EntityDef {

  val CurrentVersion: Int = 1

  sealed abstract class Kind(val name: String)

  object Kind {
    case object Root extends Kind("root")
    case object Value extends Kind("value")

    val all: List[Kind] = List(Root, Value)

    implicit val encoder: Encoder[Kind] = Encoder.encodeString.contramap(_.name)
    implicit val decoder: Decoder[Kind] =
      Decoder.decodeString.emap(n => all.find(_.name == n).toRight(s"unknown kind of entity '$n': it is root or value"))
  }

  /** A field of an entity.
    *
    * @param required
    *   whether a value must have it (not null). A field with a default or computed is always there.
    * @param default
    *   the value it takes when a value does not have it.
    * @param computed
    *   how it is calculated from the other fields of the value. A value that brings it has it replaced.
    */
  final case class Field(
      name: String,
      tpe: DataType,
      required: Boolean = true,
      default: Option[Json] = None,
      constraints: Constraints = Constraints.none,
      computed: Option[Expr] = None,
      label: Option[String] = None,
      description: Option[String] = None
  )

  object Field {
    implicit val encoder: Encoder[Field] = Encoder.instance { f =>
      compact(
        "name" -> f.name.asJson,
        "type" -> f.tpe.asJson,
        "required" -> (if (f.required) Json.Null else Json.False),
        "default" -> f.default.fold(Json.Null)(d => if (d.isNull) Json.Null else d),
        "constraints" -> (if (f.constraints.isEmpty) Json.Null else f.constraints.asJson),
        "computed" -> f.computed.asJson,
        "label" -> f.label.asJson,
        "description" -> f.description.asJson
      )
    }

    implicit val decoder: Decoder[Field] = Decoder.instance { c =>
      for {
        name <- c.get[String]("name")
        tpe <- c.get[DataType]("type")
        required <- c.getOrElse[Boolean]("required")(true)
        default <- c.get[Option[Json]]("default")
        constraints <- c.getOrElse[Constraints]("constraints")(Constraints.none)
        computed <- c.get[Option[Expr]]("computed")
        label <- c.get[Option[String]]("label")
        description <- c.get[Option[String]]("description")
      } yield Field(name, tpe, required, default.filterNot(_.isNull), constraints, computed, label, description)
    }
  }

  /** Limits on the value of a field. Each applies to one type: `min`/`max` to numbers, `minLength`/`maxLength`/`pattern` to text
    * (the pattern is found anywhere in the text unless it is anchored with `^...$`), `minItems`/`maxItems` to lists, `oneOf` to
    * any.
    */
  final case class Constraints(
      min: Option[BigDecimal] = None,
      max: Option[BigDecimal] = None,
      minLength: Option[Int] = None,
      maxLength: Option[Int] = None,
      pattern: Option[String] = None,
      minItems: Option[Int] = None,
      maxItems: Option[Int] = None,
      oneOf: List[Json] = Nil
  ) {
    def isEmpty: Boolean = this == Constraints.none
  }

  object Constraints {
    val none: Constraints = Constraints()

    implicit val encoder: Encoder[Constraints] = Encoder.instance { k =>
      compact(
        "min" -> k.min.asJson,
        "max" -> k.max.asJson,
        "minLength" -> k.minLength.asJson,
        "maxLength" -> k.maxLength.asJson,
        "pattern" -> k.pattern.asJson,
        "minItems" -> k.minItems.asJson,
        "maxItems" -> k.maxItems.asJson,
        "oneOf" -> (if (k.oneOf.isEmpty) Json.Null else k.oneOf.asJson)
      )
    }

    implicit val decoder: Decoder[Constraints] = Decoder.instance { c =>
      for {
        min <- c.get[Option[BigDecimal]]("min")
        max <- c.get[Option[BigDecimal]]("max")
        minLength <- c.get[Option[Int]]("minLength")
        maxLength <- c.get[Option[Int]]("maxLength")
        pattern <- c.get[Option[String]]("pattern")
        minItems <- c.get[Option[Int]]("minItems")
        maxItems <- c.get[Option[Int]]("maxItems")
        oneOf <- c.getOrElse[List[Json]]("oneOf")(Nil)
      } yield Constraints(min, max, minLength, maxLength, pattern.filter(_.nonEmpty), minItems, maxItems, oneOf)
    }
  }

  /** A condition that every value keeps: `check` must be true, if `when` is (no `when`: always).
    *
    * @param message
    *   what a value that breaks it is told; by default, that it breaks the rule.
    */
  final case class Rule(name: String, check: Expr, message: Option[String] = None, when: Option[Expr] = None) {
    def explain: String = message.getOrElse(s"breaks the rule '$name'")
  }

  object Rule {
    implicit val encoder: Encoder[Rule] = Encoder.instance { r =>
      compact("name" -> r.name.asJson, "when" -> r.when.asJson, "check" -> r.check.asJson, "message" -> r.message.asJson)
    }
    implicit val decoder: Decoder[Rule] = Decoder.forProduct4("name", "check", "message", "when")(Rule.apply)
  }

  /** The states of a value, kept in the text field `field`, and how it goes from one to another. A value without state starts at
    * `initial`. Values are only moved with a transition by the node `entity.transition`; a value that is given with any state is
    * valid if its state is one of `states`.
    */
  final case class StateMachine(field: String, states: List[String], initial: String, transitions: List[Transition] = Nil)

  object StateMachine {
    implicit val encoder: Encoder[StateMachine] =
      Encoder.forProduct4("field", "states", "initial", "transitions")((m: StateMachine) => (m.field, m.states, m.initial, m.transitions))
    implicit val decoder: Decoder[StateMachine] = Decoder.instance { c =>
      for {
        field <- c.get[String]("field")
        states <- c.getOrElse[List[String]]("states")(Nil)
        initial <- c.get[String]("initial")
        transitions <- c.getOrElse[List[Transition]]("transitions")(Nil)
      } yield StateMachine(field, states, initial, transitions)
    }
  }

  /** From any state of `from` to `to`, if `guard` holds for the value. `message` is what a value that does not meet the guard is
    * told.
    */
  final case class Transition(name: String, from: List[String], to: String, guard: Option[Expr] = None, message: Option[String] = None)

  object Transition {
    implicit val encoder: Encoder[Transition] = Encoder.instance { t =>
      compact("name" -> t.name.asJson, "from" -> t.from.asJson, "to" -> t.to.asJson, "guard" -> t.guard.asJson, "message" -> t.message.asJson)
    }
    implicit val decoder: Decoder[Transition] = Decoder.instance { c =>
      for {
        name <- c.get[String]("name")
        // One state may be written without a list.
        from <- c.get[List[String]]("from").orElse(c.get[String]("from").map(List(_)))
        to <- c.get[String]("to")
        guard <- c.get[Option[Expr]]("guard")
        message <- c.get[Option[String]]("message")
      } yield Transition(name, from, to, guard, message)
    }
  }

  /** An object without the fields that say nothing (null). */
  private def compact(fields: (String, Json)*): Json = Json.fromJsonObject(JsonObject.fromIterable(fields.filterNot(_._2.isNull)))

  implicit val encoder: Encoder[EntityDef] = Encoder.instance { e =>
    compact(
      "version" -> e.version.asJson,
      "name" -> e.name.asJson,
      "label" -> e.label.asJson,
      "description" -> e.description.asJson,
      "kind" -> e.kind.asJson,
      "key" -> e.key.asJson,
      "revision" -> e.revision.asJson,
      "fields" -> e.fields.asJson,
      "rules" -> (if (e.rules.isEmpty) Json.Null else e.rules.asJson),
      "states" -> e.states.asJson
    )
  }

  implicit val decoder: Decoder[EntityDef] = Decoder.instance { c =>
    for {
      version <- c.getOrElse[Int]("version")(CurrentVersion)
      name <- c.get[String]("name")
      label <- c.get[Option[String]]("label")
      description <- c.get[Option[String]]("description")
      kind <- c.getOrElse[Kind]("kind")(Kind.Root)
      key <- c.get[Option[String]]("key")
      revision <- c.getOrElse[Int]("revision")(1)
      fields <- c.getOrElse[List[Field]]("fields")(Nil)
      rules <- c.getOrElse[List[Rule]]("rules")(Nil)
      states <- c.get[Option[StateMachine]]("states")
      _ <- if (name.isEmpty) Left(DecodingFailure("the name of an entity cannot be empty", c.history)) else Right(())
    } yield EntityDef(name, fields, kind, key.filter(_.nonEmpty), rules, states, label, description, revision, version)
  }
}
