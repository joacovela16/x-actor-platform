package xplat.codec

import io.circe.{Decoder, Encoder, Printer}
import io.circe.parser.{decode => parseJson}
import xplat.core.api.Codec
import xplat.core.api.error.CodecError

import java.nio.charset.StandardCharsets.UTF_8

/** JSON codecs with circe. Import the package to get [[CodecJsonOps.fromJson]] on the `Codec` object:
  *
  * {{{
  * import io.circe.generic.semiauto._
  * import xplat.codec.circe._
  *
  * final case class Order(id: String, amount: BigDecimal)
  * implicit val orderJson: io.circe.Codec[Order] = deriveCodec
  * implicit val order: Codec.Bytes[Order] = Codec.fromJson[Order]
  * }}}
  *
  * Messages that carry a reply `ReplyTo` need the node's `RefResolver` and cannot be plain JSON values: encode the ref with
  * `resolver.serialize` into a string field.
  */
package object circe {

  implicit final class CodecJsonOps(private val codec: Codec.type) extends AnyVal {

    /** `T` as compact UTF-8 JSON. */
    def fromJson[T: Encoder: Decoder]: Codec.Bytes[T] = fromJson[T](Printer.noSpaces)

    /** `T` as UTF-8 JSON written with `printer` (e.g. `Printer.noSpaces.copy(dropNullValues = true)`). */
    def fromJson[T: Encoder: Decoder](printer: Printer): Codec.Bytes[T] =
      Codec.instance[T, Array[Byte]](
        value => Right(printer.print(Encoder[T].apply(value)).getBytes(UTF_8)),
        bytes => parseJson[T](new String(bytes, UTF_8)).left.map(e => CodecError(e.getMessage))
      )

    /** `T` as a circe `Json` value, to combine with other codecs. */
    def json[T: Encoder: Decoder]: Codec[T, io.circe.Json] =
      Codec.instance[T, io.circe.Json](value => Right(Encoder[T].apply(value)), json => Decoder[T].decodeJson(json).left.map(e => CodecError(e.getMessage)))
  }
}
