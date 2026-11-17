package xplat.flow.entity

import io.circe.{Encoder, Json}
import xplat.flow.model.{DataType, Expr}

/** Entities written in Scala. What it builds is an [[EntityDef]], the same data that the editor writes as JSON, so an entity can
  * be defined in code (in a plugin, or where the server is built) and still be seen, used and validated like any other.
  *
  * {{{
  * import xplat.flow.entity.dsl._
  *
  * val OrderLine = valueObject("OrderLine")(
  *   field("sku", str).pattern("^[A-Z0-9-]+$"),
  *   field("qty", num).min(1),
  *   field("price", num).min(0),
  *   computed("subtotal", num)(path("qty") * path("price"))
  * )
  *
  * val Order = entity("Order", key = "id")(
  *   field("id", str),
  *   field("customer", ref("Customer")),
  *   field("lines", listOf(OrderLine)).minItems(1),
  *   computed("total", num)(sum("lines")(path("subtotal"))),
  *   field("status", str),
  *   field("notes", str).optional
  * ).rule("positive-total", "The total must be positive")(path("total") > 0)
  *  .lifecycle("status", initial = "draft")(
  *    transition("approve", from = "draft", to = "approved").guard(path("total") > 0),
  *    transition("ship", from = "approved", to = "shipped")
  *  )
  * }}}
  */
object dsl {

  // ---- types ----

  val str: DataType = DataType.Str
  val num: DataType = DataType.Num
  val bool: DataType = DataType.Bool
  val any: DataType = DataType.AnyJson

  def listOf(element: DataType): DataType = DataType.ListOf(element)

  /** A list of values of `entity`, held inside. */
  def listOf(entity: EntityDef): DataType = DataType.ListOf(DataType.Entity(entity.name))

  /** A whole value of an entity, held inside. */
  def embed(entity: EntityDef): DataType = DataType.Entity(entity.name)

  def embed(entity: String): DataType = DataType.Entity(entity)

  /** A reference to a value of a root entity: the value of its key. */
  def ref(entity: EntityDef): DataType = DataType.Ref(entity.name)

  def ref(entity: String): DataType = DataType.Ref(entity)

  def obj(fields: (String, DataType)*): DataType = DataType.Obj(fields.toMap)

  // ---- fields ----

  def field(name: String, tpe: DataType): FieldBuilder = FieldBuilder(EntityDef.Field(name, tpe))

  def computed(name: String, tpe: DataType)(expr: Expr): FieldBuilder = FieldBuilder(EntityDef.Field(name, tpe, computed = Some(expr)))

  final case class FieldBuilder(field: EntityDef.Field) {
    private def constrain(f: EntityDef.Constraints => EntityDef.Constraints) = copy(field.copy(constraints = f(field.constraints)))

    def optional: FieldBuilder = copy(field.copy(required = false))
    def default[A: Encoder](value: A): FieldBuilder = copy(field.copy(default = Some(Encoder[A].apply(value))))
    def label(text: String): FieldBuilder = copy(field.copy(label = Some(text)))
    def description(text: String): FieldBuilder = copy(field.copy(description = Some(text)))
    def min(n: BigDecimal): FieldBuilder = constrain(_.copy(min = Some(n)))
    def max(n: BigDecimal): FieldBuilder = constrain(_.copy(max = Some(n)))
    def minLength(n: Int): FieldBuilder = constrain(_.copy(minLength = Some(n)))
    def maxLength(n: Int): FieldBuilder = constrain(_.copy(maxLength = Some(n)))
    def pattern(regex: String): FieldBuilder = constrain(_.copy(pattern = Some(regex)))
    def minItems(n: Int): FieldBuilder = constrain(_.copy(minItems = Some(n)))
    def maxItems(n: Int): FieldBuilder = constrain(_.copy(maxItems = Some(n)))
    def oneOf[A: Encoder](values: A*): FieldBuilder = constrain(_.copy(oneOf = values.toList.map(Encoder[A].apply)))
  }

  // ---- entities ----

  /** A root entity: it has an identity, the field `key`, and can be referenced. */
  def entity(name: String, key: String)(fields: FieldBuilder*): EntityDef =
    EntityDef(name, fields.toList.map(_.field), EntityDef.Kind.Root, Some(key))

  /** A value entity: it has no identity and only lives inside others. */
  def valueObject(name: String)(fields: FieldBuilder*): EntityDef = EntityDef(name, fields.toList.map(_.field), EntityDef.Kind.Value)

  implicit final class EntityOps(private val e: EntityDef) extends AnyVal {
    def labelled(text: String): EntityDef = e.copy(label = Some(text))
    def described(text: String): EntityDef = e.copy(description = Some(text))

    /** A condition every value keeps. */
    def rule(name: String, message: String)(check: Expr): EntityDef = e.copy(rules = e.rules :+ EntityDef.Rule(name, check, Some(message)))

    /** A condition the values for which `when` is true keep. */
    def ruleWhen(name: String, message: String)(when: Expr)(check: Expr): EntityDef =
      e.copy(rules = e.rules :+ EntityDef.Rule(name, check, Some(message), Some(when)))

    /** The states of its values, kept in `field`: the initial one and those the transitions name, in that order. */
    def lifecycle(field: String, initial: String)(transitions: TransitionBuilder*): EntityDef = {
      val ts = transitions.toList.map(_.transition)
      e.copy(states = Some(EntityDef.StateMachine(field, (initial :: ts.flatMap(t => t.from :+ t.to)).distinct, initial, ts)))
    }

    /** The same, with every state listed (also those no transition reaches). */
    def lifecycle(field: String, initial: String, all: List[String])(transitions: TransitionBuilder*): EntityDef =
      e.copy(states = Some(EntityDef.StateMachine(field, all, initial, transitions.toList.map(_.transition))))
  }

  def transition(name: String, from: String, to: String): TransitionBuilder = TransitionBuilder(EntityDef.Transition(name, List(from), to))

  def transition(name: String, from: List[String], to: String): TransitionBuilder = TransitionBuilder(EntityDef.Transition(name, from, to))

  final case class TransitionBuilder(transition: EntityDef.Transition) {
    def guard(condition: Expr): TransitionBuilder = copy(transition.copy(guard = Some(condition)))
    def message(text: String): TransitionBuilder = copy(transition.copy(message = Some(text)))
  }

  // ---- expressions ----

  /** What can stand for an expression: an expression, or a value (a number, a text, a boolean, JSON). */
  trait ToExpr[A] {
    def apply(a: A): Expr
  }

  object ToExpr {
    private def const[A: Encoder]: ToExpr[A] = a => Expr.const(Encoder[A].apply(a))
    implicit val expr: ToExpr[Expr] = e => e
    implicit val int: ToExpr[Int] = const[Int]
    implicit val long: ToExpr[Long] = const[Long]
    implicit val double: ToExpr[Double] = const[Double]
    implicit val bigDecimal: ToExpr[BigDecimal] = const[BigDecimal]
    implicit val string: ToExpr[String] = const[String]
    implicit val boolean: ToExpr[Boolean] = const[Boolean]
    implicit val json: ToExpr[Json] = j => Expr.const(j)
  }

  private def ex[A](a: A)(implicit t: ToExpr[A]): Expr = t(a)

  /** The field at `path` of the value (dotted). */
  def path(path: String): Expr = Expr.field(path)

  /** The field at `path`, or `default` when it is not there. */
  def pathOr[A: Encoder](path: String, default: A): Expr = Expr.field(path, Some(Encoder[A].apply(default)))

  def lit[A](value: A)(implicit t: ToExpr[A]): Expr = t(value)

  def exists(path: String): Expr = Expr.exists(path)

  def not(e: Expr): Expr = Expr.op("not", e)

  def len(e: Expr): Expr = Expr.op("len", e)

  def round(e: Expr, decimals: Int): Expr = Expr.op("round", e, ex(decimals))

  def ifElse[A: ToExpr, B: ToExpr](condition: Expr, yes: A, no: B): Expr = Expr.cond(condition, ex(yes), ex(no))

  def all(exprs: Expr*): Expr = Expr.op("and", exprs: _*)

  def oneHolds(exprs: Expr*): Expr = Expr.op("or", exprs: _*)

  /** The sum of `body` over the items of the list at `list`. */
  def sum(list: String)(body: Expr): Expr = Expr.over("sum", list, Some(body))

  def count(list: String): Expr = Expr.over("count", list)

  /** How many items of the list at `list` meet `condition`. */
  def countWhere(list: String)(condition: Expr): Expr = Expr.over("count", list, Some(condition))

  def minOf(list: String)(body: Expr): Expr = Expr.over("min", list, Some(body))

  def maxOf(list: String)(body: Expr): Expr = Expr.over("max", list, Some(body))

  /** Whether every item of the list at `list` meets `condition` (true for none). */
  def forAll(list: String)(condition: Expr): Expr = Expr.over("all", list, Some(condition))

  /** Whether some item of the list at `list` meets `condition`. */
  def forAny(list: String)(condition: Expr): Expr = Expr.over("any", list, Some(condition))

  implicit final class ExprOps(private val e: Expr) extends AnyVal {
    def ===[A: ToExpr](other: A): Expr = Expr.op("eq", e, ex(other))
    def =!=[A: ToExpr](other: A): Expr = Expr.op("neq", e, ex(other))
    def >[A: ToExpr](other: A): Expr = Expr.op("gt", e, ex(other))
    def >=[A: ToExpr](other: A): Expr = Expr.op("gte", e, ex(other))
    def <[A: ToExpr](other: A): Expr = Expr.op("lt", e, ex(other))
    def <=[A: ToExpr](other: A): Expr = Expr.op("lte", e, ex(other))
    def &&(other: Expr): Expr = Expr.op("and", e, other)
    def ||(other: Expr): Expr = Expr.op("or", e, other)
    def unary_! : Expr = Expr.op("not", e)
    def +[A: ToExpr](other: A): Expr = Expr.op("add", e, ex(other))
    def -[A: ToExpr](other: A): Expr = Expr.op("sub", e, ex(other))
    def *[A: ToExpr](other: A): Expr = Expr.op("mul", e, ex(other))
    def /[A: ToExpr](other: A): Expr = Expr.op("div", e, ex(other))
    def ++[A: ToExpr](other: A): Expr = Expr.op("concat", e, ex(other))
    def in[A: Encoder](values: A*): Expr = Expr.op("in", e, Expr.const(Json.fromValues(values.map(Encoder[A].apply))))
    def matches(regex: String): Expr = Expr.op("matches", e, Expr.const(Json.fromString(regex)))

    /** If this holds, `consequence` must too. */
    def implies(consequence: Expr): Expr = Expr.cond(e, consequence, Expr.const(Json.True))
  }
}
