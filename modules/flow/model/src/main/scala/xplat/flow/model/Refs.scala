package xplat.flow.model

import io.circe.Json

/** The one syntax for values taken from somewhere else, the same in nodes, connections and triggers: `${origin.path}`.
  *
  *  - `origin` says where the value comes from: `state` (the flow state), `context` (the run), `env` (environment variables of
  *    the server), `args` (the arguments a connection receives), `request` (an HTTP trigger) or `message` (a queue trigger).
  *    Each place allows only some of them.
  *  - `${env.PORT:-27017}` has a default, used when the value is missing, null or an empty text.
  *  - `$${` is a literal `${`.
  *
  * A text that is only one reference (`"${state.qty}"`) is replaced by the value with its type; a reference inside a text
  * (`"ipa-${args.domain}"`) is rendered as text (a string as it is, anything else as JSON).
  */
object Refs {

  final case class Ref(origin: String, path: List[String], default: Option[String]) {
    def render: String = (origin :: path).mkString(".") + default.fold("")(d => s":-$d")
  }

  /** Looks a reference up: `Right(None)` when it is not there. `Left` when it can never be there (for example, an origin that is
    * not allowed in that place).
    */
  type Lookup = Ref => Either[String, Option[Json]]

  private val Pattern = """\$\$\{|\$\{([^}]*)\}""".r

  /** The references of a text, or why one of them is broken. */
  def parse(text: String): Either[String, List[Ref]] =
    Pattern.findAllMatchIn(text).toList.flatMap(m => Option(m.group(1))).foldLeft[Either[String, List[Ref]]](Right(Nil)) { (acc, raw) =>
      acc.flatMap(done => one(raw).map(done :+ _))
    }

  private def one(raw: String): Either[String, Ref] = {
    val (body, default) = raw.indexOf(":-") match {
      case -1 => (raw, None)
      case i  => (raw.substring(0, i), Some(raw.substring(i + 2)))
    }
    val segments = body.trim.split("\\.", -1).toList
    if (body.trim.isEmpty || segments.exists(_.isEmpty)) Left(s"'$${$raw}' is not a reference: it must be $${origin.path}")
    else Right(Ref(segments.head, segments.tail, default))
  }

  /** A text that is only one reference, such as `"${state.qty}"`. */
  def whole(text: String): Option[Ref] =
    Pattern.findAllMatchIn(text).toList match {
      case List(m) if m.group(1) != null && m.start == 0 && m.end == text.length => one(m.group(1)).toOption
      case _                                                                      => None
    }

  /** Every reference anywhere in a JSON value (in its texts, not in its keys). */
  def in(json: Json): List[Ref] =
    json.fold(Nil, _ => Nil, _ => Nil, t => parse(t).getOrElse(Nil), _.toList.flatMap(in), _.values.toList.flatMap(in)).distinct

  /** The broken references anywhere in a JSON value, and the ones whose origin is not in `allowed` (with `where` in the message). */
  def problems(json: Json, allowed: Set[String], where: String): List[String] =
    json.fold(
      Nil,
      _ => Nil,
      _ => Nil,
      t =>
        parse(t) match {
          case Left(problem) => List(problem)
          case Right(refs) =>
            refs.filterNot(r => allowed(r.origin)).map { r =>
              s"'$${${r.render}}': '${r.origin}' cannot be used in $where (only ${allowed.toList.sorted.map(o => s"$o.").mkString(", ")})"
            }
        },
      _.toList.flatMap(problems(_, allowed, where)),
      _.values.toList.flatMap(problems(_, allowed, where))
    ).distinct

  private def text(v: Json): String = v.asString.getOrElse(v.noSpaces)

  private def value(r: Ref, lookup: Lookup): Either[String, Json] =
    lookup(r).flatMap { found =>
      found.filterNot(v => v.isNull || v.asString.contains("")) match {
        case Some(v) => Right(v)
        case None    => r.default.map(Json.fromString).toRight(s"'$${${r.render}}' has no value")
      }
    }

  /** A text with its references replaced: a whole reference by the value with its type, the others as text. `escape` is applied to
    * each value put into a text (for example, to encode it for a URL).
    */
  def resolveText(t: String, lookup: Lookup, escape: String => String = identity): Either[String, Json] =
    whole(t) match {
      case Some(r) => value(r, lookup)
      case None =>
        parse(t).flatMap { _ =>
          var problem: Option[String] = None
          val out = Pattern.replaceAllIn(
            t,
            m =>
              scala.util.matching.Regex.quoteReplacement(Option(m.group(1)) match {
                case None => "${"
                case Some(raw) =>
                  one(raw).flatMap(value(_, lookup)) match {
                    case Right(v) => escape(text(v))
                    case Left(e)  => problem = problem.orElse(Some(e)); ""
                  }
              })
          )
          problem.toLeft(Json.fromString(out))
        }
    }

  /** Every text of a JSON value with its references replaced; the first problem stops it. */
  def resolve(json: Json, lookup: Lookup): Either[String, Json] =
    json.fold(
      Right(json),
      _ => Right(json),
      _ => Right(json),
      t => resolveText(t, lookup),
      vs => vs.foldLeft[Either[String, Vector[Json]]](Right(Vector.empty))((acc, v) => acc.flatMap(d => resolve(v, lookup).map(d :+ _))).map(Json.fromValues),
      o => o.toList.foldLeft[Either[String, List[(String, Json)]]](Right(Nil))((acc, kv) => acc.flatMap(d => resolve(kv._2, lookup).map(v => d :+ (kv._1 -> v)))).map(Json.fromFields)
    )

  /** A lookup by origin; an origin that is not given cannot be used there. */
  def lookup(origins: (String, List[String] => Option[Json])*): Lookup = {
    val byName = origins.toMap
    r => byName.get(r.origin).map(f => f(r.path)).toRight(s"'$${${r.render}}': '${r.origin}' cannot be used here")
  }

  /** A lookup into a JSON value by path. */
  def at(json: Json): List[String] => Option[Json] = path => if (path.isEmpty) Some(json) else StatePath(path).get(json)
}
