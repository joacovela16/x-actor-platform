package xplat.flow.engine

import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import xplat.core.api.Codec
import xplat.core.api.error.CodecError

import java.nio.charset.StandardCharsets.UTF_8

/** What the journal stores of an instance: its events, and a snapshot of its state, as JSON text. */
private[engine] object Codecs {

  private def json[A: Encoder: Decoder]: Codec[A, Array[Byte]] =
    Codec.instance[A, Array[Byte]](
      a => Right(a.asJson.noSpaces.getBytes(UTF_8)),
      bytes => parse(new String(bytes, UTF_8)).flatMap(_.as[A]).left.map(e => CodecError(e.getMessage))
    )

  val event: Codec[InstanceEvent, Array[Byte]] = json[InstanceEvent]
  val state: Codec[InstanceState, Array[Byte]] = json[InstanceState]
}
