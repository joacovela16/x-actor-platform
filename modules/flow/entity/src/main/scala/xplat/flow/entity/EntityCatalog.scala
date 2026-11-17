package xplat.flow.entity

import io.circe.{Json, JsonObject}
import xplat.flow.model.DataType.Problem
import xplat.flow.model.{DataType, Expr, Types}

import scala.annotation.tailrec

/** The entities of the business model, as the flows see them: it answers what the types of a flow ask about an entity
  * ([[Types]]), checks values against their entity and moves them from one state to another.
  */
final class EntityCatalog private (val entities: List[EntityDef]) extends Types {

  private val byName: Map[String, EntityDef] = entities.map(e => e.name -> e).toMap

  def get(name: String): Option[EntityDef] = byName.get(name)

  def names: List[String] = entities.map(_.name).sorted

  /** The same catalog with `entity` added, or in place of the one with its name. */
  def updated(entity: EntityDef): EntityCatalog = new EntityCatalog(entities.filterNot(_.name == entity.name) :+ entity)

  def without(name: String): EntityCatalog = new EntityCatalog(entities.filterNot(_.name == name))

  /** The other entities that hold or reference `name`. */
  def usersOf(name: String): List[String] =
    entities.filter(e => e.name != name && e.fields.exists(_.tpe.entities.contains(name))).map(_.name).sorted

  def fields(entity: String): Option[Map[String, DataType]] = get(entity).map(_.fieldTypes)

  def key(entity: String): Option[DataType] = get(entity).flatMap(_.keyType)

  def conform(entity: String, value: Json): Either[List[Problem], Json] = {
    val checked = check(entity, value)
    if (checked.ok) Right(checked.value) else Left(checked.problems)
  }

  /** `value` as `entity` makes it, and every problem it has.
    *
    * In order: the defaults of the fields it does not have (null is not having it) and the initial state; the value of each field
    * as its type makes it (values of other entities inside it are made the same way, so their computed fields are there); the
    * computed fields, those that others use first; and then the constraints of the fields, the state and the rules. Fields that
    * the entity does not declare are kept as they are.
    */
  def check(entity: String, value: Json): EntityCatalog.Checked =
    get(entity) match {
      case None => EntityCatalog.Checked(value, List(Problem(Nil, "unknown-entity", s"the entity '$entity' does not exist")))
      case Some(e) =>
        value.asObject match {
          case None        => EntityCatalog.Checked(value, List(Problem(Nil, "type", s"must be an object (a value of $entity)")))
          case Some(o)     => checkObject(e, o)
        }
    }

  /** `value` moved by the transition `name` of `entity`: it must be a valid value whose state is one the transition starts from,
    * meet the guard of the transition, and still be valid in the new state.
    */
  def transition(entity: String, value: Json, name: String): Either[List[Problem], Json] = {
    def fail(path: List[String], rule: String, message: String): Either[List[Problem], Json] = Left(List(Problem(path, rule, message)))
    get(entity) match {
      case None => fail(Nil, "unknown-entity", s"the entity '$entity' does not exist")
      case Some(e) =>
        e.states match {
          case None => fail(Nil, "transition", s"the entity '$entity' has no states")
          case Some(m) =>
            m.transitions.find(_.name == name) match {
              case None => fail(Nil, "transition", s"the entity '$entity' has no transition '$name'")
              case Some(t) =>
                conform(entity, value).flatMap { current =>
                  val state = current.hcursor.get[String](m.field).getOrElse("")
                  if (!t.from.contains(state)) fail(List(m.field), "transition", s"cannot '$name' from '$state': only from ${t.from.mkString(", ")}")
                  else
                    t.guard.fold[Either[String, Boolean]](Right(true))(EntityCatalog.holds(_, current)) match {
                      case Left(why)    => fail(Nil, name, s"the condition of '$name' cannot be checked: $why")
                      case Right(false) => fail(Nil, name, t.message.getOrElse(s"the condition of '$name' does not hold"))
                      case Right(true)  => conform(entity, current.mapObject(_.add(m.field, Json.fromString(t.to))))
                    }
                }
            }
        }
    }
  }

  private def present(o: JsonObject, name: String): Option[Json] = o(name).filterNot(_.isNull)

  private def checkObject(e: EntityDef, value: JsonObject): EntityCatalog.Checked = {
    val defaulted = e.fields.foldLeft(value)((o, f) => if (present(o, f.name).isEmpty) f.default.fold(o)(o.add(f.name, _)) else o)
    val started = e.states.fold(defaulted) { m =>
      if (present(defaulted, m.field).isEmpty) defaulted.add(m.field, Json.fromString(m.initial)) else defaulted
    }

    val (held, fieldProblems) = e.fields.filter(_.computed.isEmpty).foldLeft((started, List.empty[Problem])) { case ((o, ps), f) =>
      present(o, f.name) match {
        case None if f.required => (o, ps :+ Problem(List(f.name), "required", "is missing"))
        case None               => (o, ps)
        case Some(v) =>
          f.tpe.conform(v, this) match {
            case Right(made) => (o.add(f.name, made), ps)
            case Left(more)  => (o, ps ++ more.map(_.under(f.name)))
          }
      }
    }

    val (computed, computeProblems) = EntityCatalog.computeOrder(e).foldLeft((held, List.empty[Problem])) { case ((o, ps), f) =>
      val made = f.computed.toRight("no expression").flatMap(_.eval(Json.fromJsonObject(o))).flatMap { v =>
        f.tpe.conform(v, this).left.map(problems => s"${v.noSpaces} is not ${f.tpe.render} (${problems.map(_.render).mkString("; ")})")
      }
      made match {
        case Right(v)  => (o.add(f.name, v), ps)
        case Left(why) => (o.remove(f.name), ps :+ Problem(List(f.name), "computed", s"cannot be computed: $why"))
      }
    }

    val json = Json.fromJsonObject(computed)
    val constraintProblems = e.fields.flatMap(f => present(computed, f.name).toList.flatMap(EntityCatalog.constraints(f, _)))
    val stateProblems = e.states.toList.flatMap { m =>
      present(computed, m.field) match {
        case Some(v) if v.asString.exists(m.states.contains) => Nil
        case Some(_) => List(Problem(List(m.field), "state", s"must be one of ${m.states.mkString(", ")}"))
        case None    => Nil
      }
    }
    // A computed field or a rule that cannot be evaluated because of another problem says nothing new.
    val before = fieldProblems ++ (if (fieldProblems.isEmpty) computeProblems else Nil) ++ constraintProblems ++ stateProblems
    val ruleProblems = e.rules.flatMap { r =>
      r.when.fold[Either[String, Boolean]](Right(true))(EntityCatalog.holds(_, json)).flatMap(applies => if (applies) EntityCatalog.holds(r.check, json) else Right(true)) match {
        case Right(true)  => Nil
        case Right(false) => List(Problem(Nil, r.name, r.explain))
        case Left(why)    => if (before.isEmpty) List(Problem(Nil, r.name, s"cannot be checked: $why")) else Nil
      }
    }
    EntityCatalog.Checked(json, before ++ ruleProblems)
  }
}

object EntityCatalog {

  val empty: EntityCatalog = new EntityCatalog(Nil)

  /** The catalog of these entities; of two with the same name, the last one. */
  def apply(entities: Iterable[EntityDef]): EntityCatalog = entities.foldLeft(empty)(_.updated(_))

  /** A value as its entity makes it (as far as it could) and what is wrong with it. */
  final case class Checked(value: Json, problems: List[Problem]) {
    def ok: Boolean = problems.isEmpty
  }

  /** Whether `expr` is true for `value`; it must give a boolean. */
  def holds(expr: Expr, value: Json): Either[String, Boolean] =
    expr.eval(value).flatMap(j => j.asBoolean.toRight(s"it gives ${j.noSpaces}, not a boolean"))

  /** The computed fields of `e` in the order to compute them: a field after the computed fields it reads. Fields that read each
    * other in a circle come last, in the order they are declared (the validator does not let such an entity be saved).
    */
  def computeOrder(e: EntityDef): List[EntityDef.Field] = {
    val (ordered, stuck) = sortComputed(e)
    ordered ++ stuck
  }

  /** The computed fields of `e` that cannot be computed because they read each other in a circle (or read one that does). */
  def computeCycle(e: EntityDef): List[String] = sortComputed(e)._2.map(_.name)

  private def sortComputed(e: EntityDef): (List[EntityDef.Field], List[EntityDef.Field]) = {
    val computed = e.fields.filter(_.computed.isDefined)
    val names = computed.map(_.name).toSet
    def reads(f: EntityDef.Field): List[String] = f.computed.toList.flatMap(_.paths).flatMap(_.segments.headOption).filter(names).distinct

    @tailrec
    def loop(pending: List[EntityDef.Field], done: List[EntityDef.Field]): (List[EntityDef.Field], List[EntityDef.Field]) = {
      val ready = pending.filter(f => reads(f).forall(n => done.exists(_.name == n)))
      if (pending.isEmpty || ready.isEmpty) (done, pending)
      else loop(pending.filterNot(ready.contains), done ++ ready)
    }
    loop(computed, Nil)
  }

  private def count(n: Int, noun: String): String = if (n == 1) s"1 $noun" else s"$n ${noun}s"

  private def constraints(f: EntityDef.Field, v: Json): List[Problem] = {
    val k = f.constraints
    def problem(rule: String, message: String) = Problem(List(f.name), rule, message)
    val number = v.asNumber.flatMap(_.toBigDecimal)
    val text = v.asString
    val items = v.asArray
    List(
      for { m <- k.min; n <- number if n < m } yield problem("min", s"must be at least $m"),
      for { m <- k.max; n <- number if n > m } yield problem("max", s"must be at most $m"),
      for { m <- k.minLength; t <- text if t.length < m } yield problem("minLength", s"must have at least ${count(m, "character")}"),
      for { m <- k.maxLength; t <- text if t.length > m } yield problem("maxLength", s"must have at most ${count(m, "character")}"),
      for { p <- k.pattern; t <- text if !Expr.matches(t, p).getOrElse(false) } yield problem("pattern", s"must match $p"),
      for { m <- k.minItems; is <- items if is.size < m } yield problem("minItems", s"must have at least ${count(m, "item")}"),
      for { m <- k.maxItems; is <- items if is.size > m } yield problem("maxItems", s"must have at most ${count(m, "item")}"),
      if (k.oneOf.nonEmpty && !k.oneOf.exists(Expr.same(_, v))) Some(problem("oneOf", s"must be one of ${k.oneOf.map(_.noSpaces).mkString(", ")}")) else None
    ).flatten
  }
}
