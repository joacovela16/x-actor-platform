package xplat.core.api

import xplat.core.api.error.CodecError

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.util.Try

/** Turns values into their stored or transmitted form and back. Most codecs of the library are `Codec[T, Array[Byte]]`
  * (see [[Codec.Bytes]]); build them from text or JSON instead of writing bytes by hand:
  *
  * {{{
  * implicit val orderId: Codec.Bytes[OrderId] = Codec.fromString[OrderId](_.value)(s => Right(OrderId(s)))
  * implicit val order: Codec.Bytes[Order] = Codec.fromJson[Order] // with xplat-circe: import xplat.codec.circe._
  * }}}
  */
trait Codec[From, To] {
  def encode(from: From): Either[CodecError, To]

  def decode(to: To): Either[CodecError, From]

  /** The same format for another type that converts both ways without failing. */
  def imap[B](f: From => B)(g: B => From): Codec[B, To] = Codec.instance[B, To](b => encode(g(b)), to => decode(to).map(f))

  /** The same format for another type, when reading may fail (e.g. validation). */
  def emap[B](f: From => Either[String, B])(g: B => From): Codec[B, To] =
    Codec.instance[B, To](b => encode(g(b)), to => decode(to).flatMap(a => f(a).left.map(CodecError(_))))

  /** Encodes with this codec, then with `next`; decodes the other way round. */
  def andThen[C](next: Codec[To, C]): Codec[From, C] =
    Codec.instance[From, C](from => encode(from).flatMap(next.encode), c => next.decode(c).flatMap(decode))
}

object Codec {

  /** A codec to bytes, the form used by journals, cluster messages and brokers. */
  type Bytes[T] = Codec[T, Array[Byte]]

  def apply[A, B](implicit codec: Codec[A, B]): Codec[A, B] = codec

  def instance[A, B](encode: A => Either[CodecError, B], decode: B => Either[CodecError, A]): Codec[A, B] = {
    val (enc, dec) = (encode, decode)
    new Codec[A, B] {
      def encode(from: A): Either[CodecError, B] = enc(from)
      def decode(to: B): Either[CodecError, A] = dec(to)
    }
  }

  /** A codec from two functions, where only reading may fail. Exceptions thrown by `decode` become errors too. */
  def from[A, B](encode: A => B)(decode: B => Either[String, A]): Codec[A, B] =
    instance[A, B](a => Right(encode(a)), b => Try(decode(b)).toEither.left.map(_.toString).flatMap(identity).left.map(CodecError(_)))

  /** The bytes as they are. */
  implicit val bytes: Bytes[Array[Byte]] = instance[Array[Byte], Array[Byte]](Right(_), Right(_))

  /** Text as UTF-8. */
  implicit val utf8: Bytes[String] = instance[String, Array[Byte]](s => Right(s.getBytes(UTF_8)), b => Right(new String(b, UTF_8)))

  /** A value written as UTF-8 text: `Codec.fromString[OrderId](_.value)(s => Right(OrderId(s)))`. */
  def fromString[T](encode: T => String)(decode: String => Either[String, T]): Bytes[T] =
    from[T, String](encode)(decode).andThen(utf8)

  // Basic types, as text (e.g. 42 is stored as "42"), so they stay readable in the database or the broker.
  implicit val int: Bytes[Int] = fromString[Int](_.toString)(s => s.toIntOption.toRight(s"not an int: $s"))
  implicit val long: Bytes[Long] = fromString[Long](_.toString)(s => s.toLongOption.toRight(s"not a long: $s"))
  implicit val double: Bytes[Double] = fromString[Double](_.toString)(s => s.toDoubleOption.toRight(s"not a double: $s"))
  implicit val boolean: Bytes[Boolean] = fromString[Boolean](_.toString)(s => s.toBooleanOption.toRight(s"not a boolean: $s"))
  implicit val bigDecimal: Bytes[BigDecimal] = fromString[BigDecimal](_.toString)(s => Try(BigDecimal(s)).toOption.toRight(s"not a decimal: $s"))
  implicit val uuid: Bytes[UUID] = fromString[UUID](_.toString)(s => Try(UUID.fromString(s)).toOption.toRight(s"not a UUID: $s"))
  implicit val unit: Bytes[Unit] = instance[Unit, Array[Byte]](_ => Right(Array.emptyByteArray), _ => Right(()))
}
