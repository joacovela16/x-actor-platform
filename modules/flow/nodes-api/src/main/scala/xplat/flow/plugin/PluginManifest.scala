package xplat.flow.plugin

import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Encoder, Json}

/** The `flow-plugin.json` of a plugin.
  *
  * {{{
  * { "name": "ipa", "version": "1.4.2", "apiVersion": "1.0.0", "transport": "jvm" }
  * { "name": "geo", "version": "0.3.0", "apiVersion": "1.0.0", "transport": "process", "command": ["python3", "main.py"] }
  * }}}
  *
  * @param name
  *   lower case letters, digits and `-`; the prefix of the kinds of the plugin.
  * @param command
  *   for `process`: the executable and its arguments, run in the plugin's directory.
  */
final case class PluginManifest(
    name: String,
    version: PluginManifest.Version,
    apiVersion: PluginManifest.Version,
    transport: PluginManifest.Transport,
    command: List[String] = Nil
) {

  /** The full name of one of its kinds of node: `ipa.lookup@1`. */
  def kindName(kind: String): String = s"$name.$kind@${version.major}"

  /** `ipa@1`: one installed major of a plugin. */
  def id: String = s"$name@${version.major}"
}

object PluginManifest {

  val FileName: String = "flow-plugin.json"

  private val NameRe = "[a-z][a-z0-9-]*".r

  /** A semantic version, `major.minor.patch` (a `-suffix` is kept but not compared). */
  final case class Version(major: Int, minor: Int, patch: Int, suffix: String = "") {
    override def toString: String = s"$major.$minor.$patch$suffix"
  }

  object Version {
    private val Re = """(\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?""".r

    def parse(s: String): Either[String, Version] = s match {
      case Re(ma, mi, pa, suf) => Right(Version(ma.toInt, mi.toInt, pa.toInt, Option(suf).getOrElse("")))
      case _                   => Left(s"'$s' is not a version (major.minor.patch)")
    }

    implicit val ordering: Ordering[Version] = Ordering.by(v => (v.major, v.minor, v.patch))
    implicit val decoder: Decoder[Version] = Decoder.decodeString.emap(parse)
    implicit val encoder: Encoder[Version] = Encoder.encodeString.contramap(_.toString)
  }

  sealed trait Transport
  object Transport {
    case object Jvm extends Transport
    case object Process extends Transport

    implicit val decoder: Decoder[Transport] = Decoder.decodeString.emap {
      case "jvm"     => Right(Jvm)
      case "process" => Right(Process)
      case other     => Left(s"transport must be jvm or process, not '$other'")
    }
    implicit val encoder: Encoder[Transport] = Encoder.encodeString.contramap {
      case Jvm     => "jvm"
      case Process => "process"
    }
  }

  implicit val decoder: Decoder[PluginManifest] = Decoder.instance { c =>
    for {
      name <- c.get[String]("name").flatMap { n =>
        if (NameRe.matches(n)) Right(n) else Left(DecodingFailure(s"name '$n' must be lower case letters, digits and '-'", c.history))
      }
      version <- c.get[Version]("version")
      api <- c.get[Version]("apiVersion")
      transport <- c.getOrElse[Transport]("transport")(Transport.Jvm)
      command <- c.getOrElse[List[String]]("command")(Nil)
      _ <- Either.raiseWhen(transport == Transport.Process && command.isEmpty)(DecodingFailure("a process plugin needs a command", c.history))
    } yield PluginManifest(name, version, api, transport, command)
  }

  implicit val encoder: Encoder[PluginManifest] = Encoder.instance { m =>
    Json.obj(
      "name" -> Json.fromString(m.name),
      "version" -> Encoder[Version].apply(m.version),
      "apiVersion" -> Encoder[Version].apply(m.apiVersion),
      "transport" -> Encoder[Transport].apply(m.transport),
      "command" -> Json.fromValues(m.command.map(Json.fromString))
    )
  }
}
