package xplat.flow.http

import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Decoder, Json}
import io.circe.parser.parse

/** A [[Bundle]] as a directory: one file per item, so that a change to one flow is a change to one file in git.
  *
  * {{{
  * bundle.json              format, version, requires
  * flows/<id>.json          {flow, enabled}
  * triggers/<id>.json
  * entities/<name>.json
  * connections/<name>.json
  * }}}
  *
  * Paths use `/` and are relative to the directory. The text of each file is the stable one of [[Bundle.printer]].
  */
object BundleFiles {

  val Index: String = "bundle.json"

  /** The folders of the items, which an export owns: a file in them that is not in the bundle is a deleted item. */
  val Folders: List[String] = List("flows", "triggers", "entities", "connections")

  private val SafeName = "[A-Za-z0-9_.@-]+".r

  private def name(folder: String, id: String): Either[String, String] =
    if (SafeName.matches(id) && !id.startsWith(".")) Right(s"$folder/$id.json")
    else Left(s"'$id' cannot be a file name in $folder/ (letters, digits, '_', '.', '@' and '-')")

  private def text(json: Json): String = Bundle.printer.print(json) + "\n"

  /** The files of `bundle`, by path. */
  def write(bundle: Bundle): Either[String, Map[String, String]] = {
    val whole = bundle.asJson
    def items(field: String, folder: String, id: Json => String): Either[String, List[(String, String)]] =
      whole.hcursor.downField(field).as[List[Json]].getOrElse(Nil).traverse(j => name(folder, id(j)).map(_ -> text(j)))
    def str(path: String*)(j: Json): String = path.foldLeft(j.hcursor: io.circe.ACursor)(_.downField(_)).as[String].getOrElse("")
    for {
      flows <- items("flows", "flows", str("flow", "id"))
      triggers <- items("triggers", "triggers", str("id"))
      entities <- items("entities", "entities", str("name"))
      connections <- items("connections", "connections", str("name"))
      index = whole.mapObject(_.filterKeys(k => !Folders.contains(k)))
    } yield (List(Index -> text(index)) ++ flows ++ triggers ++ entities ++ connections).toMap
  }

  /** The bundle that `files` (by path) make. Files outside the folders, or that are not `.json`, are ignored. */
  def read(files: Map[String, String]): Either[String, Bundle] = {
    def json(path: String): Either[String, Json] = files.get(path).toRight(s"$path is missing").flatMap(t => parse(t).leftMap(e => s"$path: ${e.message}"))
    def all[A: Decoder](folder: String): Either[String, List[A]] =
      files.keys.toList.filter(p => p.startsWith(s"$folder/") && p.endsWith(".json") && !p.drop(folder.length + 1).contains("/")).sorted.traverse { p =>
        json(p).flatMap(_.as[A].leftMap(e => s"$p: ${e.getMessage}"))
      }
    for {
      index <- json(Index)
      flows <- all[Json]("flows")
      triggers <- all[Json]("triggers")
      entities <- all[Json]("entities")
      connections <- all[Json]("connections")
      whole = index.deepMerge(
        Json.obj(
          "flows" -> flows.asJson,
          "triggers" -> triggers.asJson,
          "entities" -> entities.asJson,
          "connections" -> connections.asJson
        )
      )
      bundle <- whole.as[Bundle].leftMap(e => s"$Index: ${e.getMessage}")
    } yield bundle
  }
}
