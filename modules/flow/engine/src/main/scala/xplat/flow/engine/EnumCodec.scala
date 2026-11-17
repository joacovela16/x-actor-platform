package xplat.flow.engine

import io.circe.{Codec, Decoder, DecodingFailure, Encoder}

/** JSON codec of a closed set of values written as words. */
private[engine] object EnumCodec {

  def apply[A](what: String, all: List[A])(name: A => String): Codec[A] = {
    val byName = all.map(a => name(a) -> a).toMap
    Codec.from(
      Decoder.instance(c => c.as[String].flatMap(s => byName.get(s).toRight(DecodingFailure(s"unknown $what '$s'", c.history)))),
      Encoder.encodeString.contramap(name)
    )
  }
}
