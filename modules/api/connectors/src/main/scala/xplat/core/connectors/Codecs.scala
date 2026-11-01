package xplat.core.connectors

import xplat.core.api.Codec
import xplat.core.api.error.CodecError

import java.nio.charset.StandardCharsets.UTF_8

object Codecs {

  /** UTF-8 text, handy for tests and simple payloads. */
  implicit val utf8: Codec[String, Array[Byte]] = new Codec[String, Array[Byte]] {
    def encode(from: String): Either[CodecError, Array[Byte]] = Right(from.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, String] = Right(new String(to, UTF_8))
  }
}
