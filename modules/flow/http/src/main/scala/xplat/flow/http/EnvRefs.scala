package xplat.flow.http

import io.circe.Json
import xplat.flow.model.{ConfigSchema, DataType, Refs}

/** References to environment variables in the configuration of connections, with the syntax of [[xplat.flow.model.Refs]]:
  * `${env.NAME}`, or `${env.NAME:-default}` with a value for when it is not set (or is empty). `$${` is a literal `${`. The older
  * `${NAME}` means the same. They work in any text of the parameters and in the values of the secrets, so a password can live only in
  * the environment of the server: `${env.DB_PASSWORD}`. Other origins (`state.`, `request.`...) cannot be used in a connection.
  *
  * A field that is a number or a boolean can be a reference as a whole (`"port": "${env.DB_PORT}"`): the value is read as that type.
  * They are resolved each time a node asks for the connection; a variable that is not set and has no default makes that an error.
  */
object EnvRefs {

  type Env = String => Option[String]

  val system: Env = name => sys.env.get(name)

  private val Legacy = "[A-Za-z_][A-Za-z0-9_]*"

  /** The variable a reference names: `env.NAME`, or the older `NAME`. */
  private def nameOf(r: Refs.Ref): Option[String] =
    if (r.origin == "env" && r.path.nonEmpty) Some(r.path.mkString("."))
    else if (r.path.isEmpty && r.origin.matches(Legacy)) Some(r.origin)
    else None

  private def notAllowed(r: Refs.Ref): String = s"'$${${r.render}}': a connection can only use env."

  /** The names the text refers to. */
  def names(text: String): List[String] = Refs.parse(text).getOrElse(Nil).flatMap(nameOf).distinct

  /** What is wrong with the references anywhere in a JSON value: broken ones, and origins other than `env.`. */
  def problems(json: Json): List[String] =
    json.fold(
      Nil,
      _ => Nil,
      _ => Nil,
      t =>
        Refs.parse(t) match {
          case Left(problem) => List(problem)
          case Right(refs)   => refs.filter(nameOf(_).isEmpty).map(notAllowed)
        },
      _.toList.flatMap(problems),
      _.values.toList.flatMap(problems)
    ).distinct

  /** The text with every reference replaced, or the names that are not set and have no default (or the problem of a reference that
    * is not to a variable).
    */
  def resolve(text: String, env: Env): Either[List[String], String] = {
    val missing = List.newBuilder[String]
    val lookup: Refs.Lookup = r =>
      nameOf(r) match {
        case None => Left(notAllowed(r))
        case Some(name) =>
          val found = env(name).filter(_.nonEmpty)
          if (found.isEmpty && r.default.isEmpty) missing += name
          Right(found.map(Json.fromString))
      }
    val out = Refs.resolveText(text, lookup)
    missing.result().distinct match {
      case Nil  => out.map(j => j.asString.getOrElse(j.noSpaces)).left.map(List(_))
      case some => Left(some)
    }
  }

  /** A string that is only one reference to a variable, such as `"${env.DB_PORT}"`. */
  def isWhole(text: String): Boolean = Refs.whole(text).flatMap(nameOf).isDefined

  /** Every name referred to anywhere in a JSON value. */
  def namesIn(json: Json): List[String] =
    json.fold(Nil, _ => Nil, _ => Nil, names, _.toList.flatMap(namesIn), _.values.toList.flatMap(namesIn)).distinct

  /** The parameters with their references resolved; a field of `schema` that is a number or a boolean and was a whole reference
    * gets that type.
    */
  def resolveParams(params: Json, schema: ConfigSchema, env: Env): Either[List[String], Json] = {
    val types = schema.fields.map(f => f.name -> f.tpe).toMap
    def go(j: Json, tpe: Option[DataType]): Either[List[String], Json] =
      j.asString match {
        case Some(text) =>
          resolve(text, env).map { value =>
            tpe match {
              case Some(DataType.Num) if isWhole(text)  => value.trim.toDoubleOption.fold(Json.fromString(value))(d => if (d.isWhole) Json.fromLong(d.toLong) else Json.fromDoubleOrString(d))
              case Some(DataType.Bool) if isWhole(text) => value.trim.toLowerCase.toBooleanOption.fold(Json.fromString(value))(Json.fromBoolean)
              case _                                    => Json.fromString(value)
            }
          }
        case None =>
          j.asArray match {
            case Some(vs) => collect(vs.map(go(_, None))).map(Json.fromValues)
            case None =>
              j.asObject match {
                case Some(o) => collect(o.toVector.map { case (k, v) => go(v, if (j eq params) types.get(k) else None).map(k -> _) }).map(Json.fromFields)
                case None    => Right(j)
              }
          }
      }
    go(params, None)
  }

  private def collect[A](xs: Vector[Either[List[String], A]]): Either[List[String], Vector[A]] = {
    val missing = xs.collect { case Left(m) => m }.flatten.distinct
    if (missing.nonEmpty) Left(missing.toList) else Right(xs.collect { case Right(a) => a })
  }

  /** The parameters as the schema checks them when they are saved: a field that is a whole reference stands for a value of its type,
    * because what it will be is only known when it is used.
    */
  def forValidation(params: Json, schema: ConfigSchema): Json =
    params.mapObject { o =>
      schema.fields.foldLeft(o) { (acc, f) =>
        acc(f.name).flatMap(_.asString).filter(isWhole) match {
          case None => acc
          case Some(_) =>
            val stand = f.oneOf.headOption.orElse(f.default).getOrElse(f.tpe match {
              case DataType.Num  => Json.fromInt(0)
              case DataType.Bool => Json.False
              case _             => Json.fromString("")
            })
            acc.add(f.name, stand)
        }
      }
    }
}
