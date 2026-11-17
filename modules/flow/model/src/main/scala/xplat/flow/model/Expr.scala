package xplat.flow.model

import io.circe.{Decoder, DecodingFailure, Encoder, Json}

import scala.math.BigDecimal.RoundingMode

/** A small expression over one value (an element of a stream, a value of an entity), written as JSON so it can be saved and
  * edited in a form. It is data and not code: it can read the value, compare, calculate and nothing else.
  *
  * {{{
  * {"path": "order.total"}                      the field of the value (error if it is not there)
  * {"path": "order.total", "default": 0}        the field, or 0 if it is not there
  * {"const": 5}  or  5, "x", true, null         a value
  * {"gt": [{"path": "total"}, 100]}             eq neq gt gte lt lte
  * {"and": [a, b]}  {"or": [a, b]}  {"not": a}
  * {"add": [a, b]}  sub mul div (numbers)       {"concat": [a, b]} (text)
  * {"if": [condition, then, else]}              {"exists": "a.b"}   {"len": a}   {"in": [a, [1, 2, 3]]}
  * {"matches": [a, "^[0-9]+$"]}                 whether the text has a part that the regular expression matches
  * {"round": [a, 2]}                            the number with that many decimals (half up)
  * {"sum": ["lines", {"path": "subtotal"}]}     over the list at a path, the expression evaluated on each item:
  *                                              sum count min max all any; without expression, the items themselves
  *                                              (count counts the items for which it is true; a list that is not there is empty)
  * }}}
  */
sealed trait Expr {

  def eval(value: Json): Either[String, Json]

  /** The JSON it is written as. */
  def toJson: Json

  /** The paths of the value it reads. Inside a list operator only the path of the list counts (the rest is about its items). */
  def paths: List[StatePath]

  /** What it reads that a value of type `tpe` does not have. A path inside an item of a list is looked up in the type of the items. */
  def check(tpe: DataType, types: Types = Types.empty): List[String]
}

object Expr {

  private final case class Const(value: Json) extends Expr {
    def eval(element: Json): Either[String, Json] = Right(value)
    def toJson: Json = if (value.isObject) Json.obj("const" -> value) else value
    def paths: List[StatePath] = Nil
    def check(tpe: DataType, types: Types): List[String] = Nil
  }

  private final case class Field(path: StatePath, default: Option[Json]) extends Expr {
    def eval(element: Json): Either[String, Json] =
      path.get(element).orElse(default).toRight(s"'$path' is not in the element")
    def toJson: Json = Json.fromFields(("path" -> Json.fromString(path.render)) :: default.map("default" -> _).toList)
    def paths: List[StatePath] = List(path)
    def check(tpe: DataType, types: Types): List[String] = known(path, tpe, types)
  }

  private final case class Exists(path: StatePath) extends Expr {
    def eval(element: Json): Either[String, Json] = Right(Json.fromBoolean(path.get(element).isDefined))
    def toJson: Json = Json.obj("exists" -> Json.fromString(path.render))
    def paths: List[StatePath] = List(path)
    def check(tpe: DataType, types: Types): List[String] = known(path, tpe, types)
  }

  private final case class Op(name: String, args: List[Expr]) extends Expr {
    def eval(element: Json): Either[String, Json] = all(args)(_.eval(element)).flatMap(Expr.apply(name, _))
    def toJson: Json = if (Unary(name)) Json.obj(name -> args.head.toJson) else Json.obj(name -> Json.fromValues(args.map(_.toJson)))
    def paths: List[StatePath] = args.flatMap(_.paths)
    def check(tpe: DataType, types: Types): List[String] = args.flatMap(_.check(tpe, types))
  }

  private final case class If(condition: Expr, yes: Expr, no: Expr) extends Expr {
    def eval(element: Json): Either[String, Json] =
      condition.eval(element).flatMap(c => c.asBoolean.toRight(s"the condition of 'if' is not a boolean: ${c.noSpaces}")).flatMap(b => if (b) yes.eval(element) else no.eval(element))
    def toJson: Json = Json.obj("if" -> Json.arr(condition.toJson, yes.toJson, no.toJson))
    def paths: List[StatePath] = condition.paths ++ yes.paths ++ no.paths
    def check(tpe: DataType, types: Types): List[String] = condition.check(tpe, types) ++ yes.check(tpe, types) ++ no.check(tpe, types)
  }

  /** An operator over the items of the list at `list`; `body` is evaluated with each item as the value. */
  private final case class Over(name: String, list: StatePath, body: Option[Expr]) extends Expr {
    def eval(element: Json): Either[String, Json] = {
      val items = list.get(element).filterNot(_.isNull) match {
        case None    => Right(Vector.empty)
        case Some(j) => j.asArray.toRight(s"'$list' is not a list: ${j.noSpaces}")
      }
      items.flatMap(is => all(is.toList)(item => body.fold[Either[String, Json]](Right(item))(_.eval(item)))).flatMap { values =>
        def bools = all(values)(v => v.asBoolean.toRight(s"'$name' needs booleans: ${v.noSpaces}"))
        name match {
          case "sum"   => all(values)(number).map(ns => Json.fromBigDecimal(ns.sum))
          case "count" => if (body.isEmpty) Right(Json.fromInt(values.size)) else bools.map(bs => Json.fromInt(bs.count(identity)))
          case "all"   => bools.map(bs => Json.fromBoolean(bs.forall(identity)))
          case "any"   => bools.map(bs => Json.fromBoolean(bs.exists(identity)))
          case _ =>
            values.foldLeft[Either[String, Option[Json]]](Right(None)) {
              case (Right(None), v) => compare(v, v).map(_ => Some(v))
              case (Right(Some(best)), v) =>
                compare(v, best).map(c => if ((name == "min" && c < 0) || (name == "max" && c > 0)) Some(v) else Some(best))
              case (left, _) => left
            }.map(_.getOrElse(Json.Null))
        }
      }
    }
    def toJson: Json = Json.obj(name -> Json.fromValues(Json.fromString(list.render) :: body.map(_.toJson).toList))
    def paths: List[StatePath] = List(list)
    def check(tpe: DataType, types: Types): List[String] =
      tpe.at(list.segments, types) match {
        case None                        => List(s"'$list' is not a field")
        case Some(DataType.ListOf(item)) => body.toList.flatMap(_.check(item, types))
        case Some(DataType.AnyJson)      => Nil
        case Some(other)                 => List(s"'$list' is ${other.render}, not a list")
      }
  }

  private def known(path: StatePath, tpe: DataType, types: Types): List[String] =
    if (tpe.at(path.segments, types).isDefined) Nil else List(s"'$path' is not a field")

  private val Binary = Set("eq", "neq", "gt", "gte", "lt", "lte", "in", "matches", "round")
  private val Variadic = Set("and", "or", "add", "sub", "mul", "div", "concat")
  private val Unary = Set("not", "len")
  private val Collection = Set("sum", "count", "min", "max", "all", "any")

  /** Every operator name, which the editor offers. */
  val Operators: Set[String] = Binary ++ Variadic ++ Unary ++ Collection

  private def all[A, B](items: List[A])(f: A => Either[String, B]): Either[String, List[B]] =
    items.foldRight[Either[String, List[B]]](Right(Nil))((a, acc) => for { rest <- acc; b <- f(a) } yield b :: rest)

  def parse(json: Json): Either[String, Expr] =
    json.asObject match {
      case None => Right(Const(json))
      case Some(o) =>
        o.toList match {
          case ("const", v) :: Nil => Right(Const(v))
          case ("path", p) :: rest if rest.forall(_._1 == "default") =>
            for {
              text <- p.asString.toRight("'path' must be text")
              path <- StatePath.parse(text)
            } yield Field(path, rest.headOption.map(_._2))
          case ("exists", p) :: Nil              => p.asString.toRight("'exists' must be a path").flatMap(StatePath.parse).map(Exists(_))
          case (name, a) :: Nil if Unary(name) => parse(a).map(x => Op(name, List(x)))
          case ("if", a) :: Nil =>
            a.asArray.map(_.toList) match {
              case Some(c :: y :: n :: Nil) => for { c1 <- parse(c); y1 <- parse(y); n1 <- parse(n) } yield If(c1, y1, n1)
              case _                        => Left("'if' takes [condition, then, else]")
            }
          case (name, a) :: Nil if Collection(name) =>
            a.asArray.map(_.toList) match {
              case Some(p :: rest) if rest.size <= 1 =>
                for {
                  text <- p.asString.toRight(s"'$name' takes the path of a list first")
                  path <- StatePath.parse(text)
                  body <- rest.headOption.fold[Either[String, Option[Expr]]](Right(None))(parse(_).map(Some(_)))
                } yield Over(name, path, body)
              case _ => Left(s"'$name' takes [path of a list, expression]")
            }
          case (name, a) :: Nil if Binary(name) || Variadic(name) =>
            a.asArray.map(_.toList).toRight(s"'$name' takes a list").flatMap { items =>
              all(items)(parse).flatMap { args =>
                if (Binary(name) && args.size != 2) Left(s"'$name' takes two values")
                else if (args.size < 2 && name != "and" && name != "or") Left(s"'$name' takes at least two values")
                else
                  (name, args) match {
                    case ("matches", List(_, Const(p))) => regex(p).map(_ => Op(name, args))
                    case ("matches", _)                 => Left("the pattern of 'matches' must be a fixed text")
                    case _                              => Right(Op(name, args))
                  }
              }
            }
          case other => Left(s"not an expression: ${Json.fromJsonObject(o).noSpaces}" + (if (other.isEmpty) " (empty)" else ""))
        }
    }

  private def number(j: Json): Either[String, BigDecimal] = j.asNumber.flatMap(_.toBigDecimal).toRight(s"not a number: ${j.noSpaces}")

  private def regex(pattern: Json): Either[String, java.util.regex.Pattern] =
    pattern.asString.toRight(s"not a pattern: ${pattern.noSpaces}").flatMap { p =>
      try Right(java.util.regex.Pattern.compile(p))
      catch { case e: java.util.regex.PatternSyntaxException => Left(s"not a valid pattern '$p': ${e.getDescription}") }
    }

  /** Whether `text` has a part that `pattern` matches (anchor it with `^...$` to match the whole text). */
  def matches(text: String, pattern: String): Either[String, Boolean] = regex(Json.fromString(pattern)).map(_.matcher(text).find())

  /** Equality that sees 1 and 1.0 as the same. */
  def same(a: Json, b: Json): Boolean = (a.asNumber.flatMap(_.toBigDecimal), b.asNumber.flatMap(_.toBigDecimal)) match {
    case (Some(x), Some(y)) => x == y
    case _                  => a == b
  }

  /** Orders numbers and text; anything else cannot be compared. */
  def compare(a: Json, b: Json): Either[String, Int] =
    (a.asNumber.flatMap(_.toBigDecimal), b.asNumber.flatMap(_.toBigDecimal), a.asString, b.asString) match {
      case (Some(x), Some(y), _, _) => Right(x.compare(y))
      case (_, _, Some(x), Some(y)) => Right(x.compareTo(y))
      case _                        => Left(s"cannot compare ${a.noSpaces} with ${b.noSpaces}")
    }

  private def apply(name: String, values: List[Json]): Either[String, Json] = {
    def bool(j: Json): Either[String, Boolean] = j.asBoolean.toRight(s"not a boolean: ${j.noSpaces}")
    def fold(f: (BigDecimal, BigDecimal) => Either[String, BigDecimal]): Either[String, Json] =
      for {
        nums <- all(values)(number)
        total <- nums.tail.foldLeft[Either[String, BigDecimal]](Right(nums.head))((acc, n) => acc.flatMap(f(_, n)))
      } yield Json.fromBigDecimal(total)
    (name, values) match {
      case ("eq", List(a, b))  => Right(Json.fromBoolean(same(a, b)))
      case ("neq", List(a, b)) => Right(Json.fromBoolean(!same(a, b)))
      case ("gt", List(a, b))  => compare(a, b).map(c => Json.fromBoolean(c > 0))
      case ("gte", List(a, b)) => compare(a, b).map(c => Json.fromBoolean(c >= 0))
      case ("lt", List(a, b))  => compare(a, b).map(c => Json.fromBoolean(c < 0))
      case ("lte", List(a, b)) => compare(a, b).map(c => Json.fromBoolean(c <= 0))
      case ("in", List(a, b))  => b.asArray.toRight(s"'in' needs a list: ${b.noSpaces}").map(l => Json.fromBoolean(l.exists(same(a, _))))
      case ("matches", List(a, p)) =>
        for {
          text <- a.asString.toRight(s"'matches' needs a text: ${a.noSpaces}")
          pattern <- p.asString.toRight(s"not a pattern: ${p.noSpaces}")
          found <- matches(text, pattern)
        } yield Json.fromBoolean(found)
      case ("round", List(a, d)) =>
        for {
          n <- number(a)
          digits <- d.asNumber.flatMap(_.toInt).filter(_ >= 0).toRight(s"'round' needs a number of decimals: ${d.noSpaces}")
        } yield Json.fromBigDecimal(n.setScale(digits, RoundingMode.HALF_UP))
      case ("and", vs)      => vs.foldLeft[Either[String, Boolean]](Right(true))((acc, j) => acc.flatMap(a => bool(j).map(a && _))).map(Json.fromBoolean)
      case ("or", vs)       => vs.foldLeft[Either[String, Boolean]](Right(false))((acc, j) => acc.flatMap(a => bool(j).map(a || _))).map(Json.fromBoolean)
      case ("not", List(a)) => bool(a).map(b => Json.fromBoolean(!b))
      case ("add", _)       => fold((a, b) => Right(a + b))
      case ("sub", _)       => fold((a, b) => Right(a - b))
      case ("mul", _)       => fold((a, b) => Right(a * b))
      case ("div", _)       => fold((a, b) => if (b == 0) Left("division by zero") else Right(a / b))
      case ("concat", vs)   => Right(Json.fromString(vs.map(v => v.asString.getOrElse(v.noSpaces)).mkString))
      case ("len", List(a)) => a.asArray.map(_.size).orElse(a.asString.map(_.length)).orElse(a.asObject.map(_.size)).map(Json.fromInt).toRight(s"no length: ${a.noSpaces}")
      case _                => Left(s"cannot apply '$name'")
    }
  }

  /** The expression in `json`, or what is wrong with it. */
  def parseAll(json: Json): List[String] = parse(json).left.toOption.toList

  // Builders, for code that writes expressions (the DSL of the entities). They give what `parse` would give for the same JSON.

  def const(value: Json): Expr = Const(value)

  /** The field at `path`; a path that is not valid is an `IllegalArgumentException`. */
  def field(path: String, default: Option[Json] = None): Expr = Field(validPath(path), default)

  def exists(path: String): Expr = Exists(validPath(path))

  /** `eq neq gt gte lt lte in matches round` (two arguments), `and or add sub mul div concat` (any number), `not len` (one). */
  def op(name: String, args: Expr*): Expr = {
    require(Binary(name) || Variadic(name) || Unary(name), s"'$name' is not an operator")
    require(!Binary(name) || args.size == 2, s"'$name' takes two values")
    require(!Unary(name) || args.size == 1, s"'$name' takes one value")
    Op(name, args.toList)
  }

  def cond(condition: Expr, yes: Expr, no: Expr): Expr = If(condition, yes, no)

  /** `sum count min max all any` over the list at `list`, with `body` evaluated on each item. */
  def over(name: String, list: String, body: Option[Expr] = None): Expr = {
    require(Collection(name), s"'$name' is not an operator over a list")
    Over(name, validPath(list), body)
  }

  private def validPath(text: String): StatePath = StatePath.parse(text).fold(m => throw new IllegalArgumentException(m), identity)

  implicit val encoder: Encoder[Expr] = Encoder.instance(_.toJson)
  implicit val decoder: Decoder[Expr] = Decoder.instance(c => c.as[Json].flatMap(j => parse(j).left.map(DecodingFailure(_, c.history))))
}
