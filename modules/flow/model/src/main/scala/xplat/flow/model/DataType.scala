package xplat.flow.model

import io.circe.{Decoder, DecodingFailure, Encoder, Json, KeyDecoder}

/** The type of a value in the state or in the configuration of a node. A single system of types is used for both.
  *
  * Besides the structural types there are the entities of the business model: [[DataType.Entity]] is a whole value of an entity
  * (its fields, nested values and rules) and [[DataType.Ref]] a reference to one (the value of its key). What an entity is comes
  * from a [[Types]]; every operation takes one, and without it no entity is known.
  */
sealed trait DataType {

  import DataType._

  /** Whether `json` is a value of this type. Objects may carry fields that the type does not mention. */
  def accepts(json: Json, types: Types = Types.empty): Boolean = conform(json, types).isRight

  /** `json` as a value of this type: the values of entities inside it with their defaults and computed fields, or every problem
    * it has, each with the path where it is.
    */
  def conform(json: Json, types: Types = Types.empty): Either[List[Problem], Json] = this match {
    case Bool    => if (json.isBoolean) Right(json) else Left(List(mismatch))
    case Num     => if (json.isNumber) Right(json) else Left(List(mismatch))
    case Str     => if (json.isString) Right(json) else Left(List(mismatch))
    case AnyJson => Right(json)
    case ListOf(element) =>
      json.asArray match {
        case None => Left(List(mismatch))
        case Some(items) =>
          val each = items.zipWithIndex.map { case (item, i) => element.conform(item, types).left.map(_.map(_.under(i.toString))) }
          val problems = each.collect { case Left(ps) => ps }.flatten.toList
          if (problems.isEmpty) Right(Json.fromValues(each.collect { case Right(v) => v })) else Left(problems)
      }
    case Obj(fields) =>
      json.asObject match {
        case None => Left(List(mismatch))
        case Some(o) =>
          val each = fields.toList.sortBy(_._1).map { case (name, t) =>
            o(name) match {
              case None    => Left(List(Problem(List(name), "required", "is missing")))
              case Some(v) => t.conform(v, types).map(name -> _).left.map(_.map(_.under(name)))
            }
          }
          val problems = each.collect { case Left(ps) => ps }.flatten
          if (problems.isEmpty) Right(Json.fromJsonObject(each.collect { case Right((n, v)) => (n, v) }.foldLeft(o)((acc, f) => acc.add(f._1, f._2))))
          else Left(problems)
      }
    case Entity(name) => types.conform(name, json)
    case Ref(entity) =>
      val ok = types.key(entity) match {
        case Some(key) => key.accepts(json, types)
        case None      => json.isString || json.isNumber
      }
      if (ok) Right(json) else Left(List(mismatch))
  }

  /** The type of the field at `path` inside a value of this type. An entity that is not known is taken as any value: the
    * validator reports it apart.
    */
  def at(path: List[String], types: Types = Types.empty): Option[DataType] = (path, this) match {
    case (Nil, _)                       => Some(this)
    case (_, AnyJson)                   => Some(AnyJson)
    case (name :: rest, Obj(fs))        => fs.get(name).flatMap(_.at(rest, types))
    case (name :: rest, Entity(entity)) =>
      types.fields(entity) match {
        case None     => Some(AnyJson)
        case Some(fs) => fs.get(name).flatMap(_.at(rest, types))
      }
    case _ => None
  }

  /** Whether a value of this type can be used where `required` is expected.
    *
    * An entity is only its own entity, or an object whose fields it has; an object is never an entity, because nothing says it
    * keeps its rules. A reference and the type of the key of its entity can be used for each other.
    */
  def conformsTo(required: DataType, types: Types = Types.empty): Boolean = (this, required) match {
    case (_, AnyJson)                     => true
    case (ListOf(a), ListOf(b))           => a.conformsTo(b, types)
    case (Entity(a), Entity(b))           => a == b
    case (Entity(a), Obj(need))           => types.fields(a).exists(have => covers(have, need, types))
    case (Obj(have), Obj(need))           => covers(have, need, types)
    case (Ref(a), Ref(b))                 => a == b
    case (Ref(a), other)                  => keysOf(a, types).contains(other)
    case (other, Ref(b))                  => keysOf(b, types).contains(other)
    case (a, b)                           => a == b
  }

  /** The names of the entities that this type mentions, at any depth. */
  def entities: Set[String] = this match {
    case ListOf(element) => element.entities
    case Obj(fields)     => fields.values.flatMap(_.entities).toSet
    case Entity(name)    => Set(name)
    case Ref(entity)     => Set(entity)
    case _               => Set.empty
  }

  def render: String

  private def mismatch: Problem = Problem(Nil, "type", s"must be $render")
}

object DataType {

  case object Bool extends DataType {
    def render: String = "bool"
  }

  case object Num extends DataType {
    def render: String = "num"
  }

  case object Str extends DataType {
    def render: String = "str"
  }

  /** Any JSON value. */
  case object AnyJson extends DataType {
    def render: String = "any"
  }

  final case class ListOf(element: DataType) extends DataType {
    def render: String = s"list[${element.render}]"
  }

  /** An object with at least these fields. */
  final case class Obj(fields: Map[String, DataType]) extends DataType {
    def render: String = fields.map { case (name, t) => s"$name: ${t.render}" }.mkString("{", ", ", "}")
  }

  /** A whole value of the entity called `name`: its fields, with the values of the entities it holds, and its rules. */
  final case class Entity(name: String) extends DataType {
    def render: String = name
  }

  /** A reference to a value of the entity `entity`: the value of its key. */
  final case class Ref(entity: String) extends DataType {
    def render: String = s"ref $entity"
  }

  /** What is wrong with a value: where (the path inside it, empty for the value itself), which rule it breaks and why.
    *
    * @param rule
    *   `type`, `required`, the name of a constraint (`min`, `pattern`...), `state`, or the name of a rule of an entity.
    */
  final case class Problem(path: List[String], rule: String, message: String) {

    /** The same problem seen from the value that holds this one at `segment`. */
    def under(segment: String): Problem = copy(path = segment :: path)

    def render: String = if (path.isEmpty) message else s"${path.mkString(".")}: $message"
  }

  object Problem {
    implicit val encoder: Encoder[Problem] =
      Encoder.forProduct3("path", "rule", "message")((p: Problem) => (p.path.mkString("."), p.rule, p.message))
    implicit val decoder: Decoder[Problem] =
      Decoder.forProduct3("path", "rule", "message")((path: String, rule: String, message: String) =>
        Problem(if (path.isEmpty) Nil else path.split("\\.", -1).toList, rule, message)
      )
  }

  /** The types a reference to `entity` can be used as: the type of its key or, for an entity that is not known, text or a number. */
  private def keysOf(entity: String, types: Types): List[DataType] = types.key(entity).fold(List[DataType](Str, Num))(List(_))

  private def covers(have: Map[String, DataType], need: Map[String, DataType], types: Types): Boolean =
    need.forall { case (name, t) => have.get(name).exists(_.conformsTo(t, types)) }

  private def tagged(name: String, more: (String, Json)*): Json =
    Json.obj(("type" -> Json.fromString(name)) +: more: _*)

  implicit lazy val encoder: Encoder[DataType] = Encoder.instance {
    case Bool         => tagged("bool")
    case Num          => tagged("num")
    case Str          => tagged("str")
    case AnyJson      => tagged("any")
    case ListOf(elem) => tagged("list", "element" -> encoder(elem))
    case Obj(fields)  => tagged("obj", "fields" -> Json.fromFields(fields.map { case (name, t) => name -> encoder(t) }))
    case Entity(name) => tagged("entity", "name" -> Json.fromString(name))
    case Ref(entity)  => tagged("ref", "entity" -> Json.fromString(entity))
  }

  implicit lazy val decoder: Decoder[DataType] = Decoder.instance { c =>
    def ok(t: DataType): Decoder.Result[DataType] = Right(t)
    c.downField("type").as[String].flatMap {
      case "bool" => ok(Bool)
      case "num"  => ok(Num)
      case "str"  => ok(Str)
      case "any"  => ok(AnyJson)
      case "list" => c.downField("element").as[DataType](decoder).map(ListOf(_))
      case "obj" =>
        c.downField("fields").as[Map[String, DataType]](Decoder.decodeMap(KeyDecoder.decodeKeyString, decoder)).map(Obj(_))
      case "entity" => c.downField("name").as[String].map(Entity(_))
      case "ref"    => c.downField("entity").as[String].map(Ref(_))
      case other    => Left(DecodingFailure(s"unknown data type '$other'", c.history))
    }
  }
}
