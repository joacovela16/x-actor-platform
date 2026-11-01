package xplat.core.api

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class CodecTest extends AnyFreeSpec with Matchers {

  private def roundTrip[T](value: T)(implicit codec: Codec.Bytes[T]): Either[String, T] =
    codec.encode(value).flatMap(codec.decode).left.map(_.message)

  final case class OrderId(value: String)

  "basic types round-trip as readable text" in {
    roundTrip("héllo") shouldBe Right("héllo")
    roundTrip(42) shouldBe Right(42)
    roundTrip(42L) shouldBe Right(42L)
    roundTrip(1.5) shouldBe Right(1.5)
    roundTrip(true) shouldBe Right(true)
    roundTrip(BigDecimal("10.25")) shouldBe Right(BigDecimal("10.25"))
    val id = UUID.randomUUID()
    roundTrip(id) shouldBe Right(id)
    roundTrip(()) shouldBe Right(())
    new String(Codec[Int, Array[Byte]].encode(42).toOption.get, "UTF-8") shouldBe "42"
  }

  "bad input is an error, not an exception" in {
    Codec.int.decode("x".getBytes).left.map(_.message) shouldBe Left("not an int: x")
    Codec.from[Int, String](_.toString)(s => Right(s.toInt)).decode("x").isLeft shouldBe true
  }

  "fromString builds a codec from text conversions" in {
    implicit val orderId: Codec.Bytes[OrderId] = Codec.fromString[OrderId](_.value)(s => if (s.nonEmpty) Right(OrderId(s)) else Left("empty id"))
    roundTrip(OrderId("o-1")) shouldBe Right(OrderId("o-1"))
    orderId.decode(Array.emptyByteArray).left.map(_.message) shouldBe Left("empty id")
  }

  "imap and emap reuse a format for another type" in {
    val cents: Codec.Bytes[BigDecimal] = Codec.long.imap(c => BigDecimal(c) / 100)(d => (d * 100).toLongExact)
    roundTrip(BigDecimal("12.34"))(cents) shouldBe Right(BigDecimal("12.34"))
    val positive = Codec.int.emap(n => if (n > 0) Right(n) else Left(s"not positive: $n"))(identity)
    positive.decode("-1".getBytes).left.map(_.message) shouldBe Left("not positive: -1")
  }
}
