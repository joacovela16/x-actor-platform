package xplat.flow.ai

import io.circe.Json
import xplat.flow.model.{Refs, StatePath}

/** The text of a prompt with `${state.path}` where a value of the state goes (the older `{{path}}` still works). It is a
  * substitution and nothing more: no conditions, no loops. A string goes in as it is; any other value goes in as JSON. `$${` is a
  * literal `${`.
  */
object Template {

  private val Slot = """\{\{\s*([^{}\s]+)\s*\}\}""".r

  /** The paths that the text asks for, or the first that is not a valid path or not of the state. */
  def paths(text: String): Either[String, List[StatePath]] =
    for {
      refs <- Refs.parse(text)
      fromRefs <- refs.foldLeft[Either[String, List[StatePath]]](Right(Nil)) { (acc, r) =>
        acc.flatMap { done =>
          if (r.origin != "state") Left(s"'$${${r.render}}': a prompt can only use state.")
          else if (r.path.isEmpty) Left(s"'$${${r.render}}' needs a path, such as $${state.order.id}")
          else Right(if (r.default.isDefined) done else done :+ StatePath(r.path))
        }
      }
      legacy <- Slot.findAllMatchIn(text).map(_.group(1)).toList.distinct.foldLeft[Either[String, List[StatePath]]](Right(Nil))((acc, p) => acc.flatMap(done => StatePath.parse(p).map(done :+ _)))
    } yield (fromRefs ++ legacy).distinct

  /** The text with the values of the state in place, or the first path that is not in the state. */
  def render(text: String, state: Json): Either[String, String] =
    paths(text).flatMap { _ =>
      val legacy = Slot.findAllMatchIn(text).map(_.group(1)).toList.distinct
      legacy.flatMap(p => StatePath.parse(p).toOption).foldLeft[Either[String, Map[String, String]]](Right(Map.empty)) { (acc, path) =>
        acc.flatMap(done => path.get(state).toRight(s"'$path' is not in the state").map(v => done + (path.render -> v.asString.getOrElse(v.noSpaces))))
      }.map(values => Slot.replaceAllIn(text, m => java.util.regex.Matcher.quoteReplacement(values(m.group(1)).replace("${", "$${")))) // a value is never read again as a reference
    }.flatMap { t =>
      Refs.resolveText(t, Refs.lookup("state" -> Refs.at(state))).map(j => j.asString.getOrElse(j.noSpaces))
    }
}
