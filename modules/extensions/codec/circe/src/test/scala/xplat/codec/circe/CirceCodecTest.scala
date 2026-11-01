package xplat.codec.circe

import io.circe.Printer
import io.circe.generic.semiauto.deriveCodec
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api.Codec

import java.nio.charset.StandardCharsets.UTF_8

class CirceCodecTest extends AnyFreeSpec with Matchers {

  final case class Order(id: String, amount: BigDecimal, note: Option[String])
  implicit val orderJson: io.circe.Codec[Order] = deriveCodec

  "fromJson writes compact JSON and reads it back" in {
    val codec = Codec.fromJson[Order]
    val bytes = codec.encode(Order("o-1", BigDecimal("9.5"), None)).toOption.get
    new String(bytes, UTF_8) shouldBe """{"id":"o-1","amount":9.5,"note":null}"""
    codec.decode(bytes) shouldBe Right(Order("o-1", BigDecimal("9.5"), None))
  }

  "a printer controls the output" in {
    val codec = Codec.fromJson[Order](Printer.noSpaces.copy(dropNullValues = true))
    new String(codec.encode(Order("o-1", 1, None)).toOption.get, UTF_8) shouldBe """{"id":"o-1","amount":1}"""
  }

  "invalid JSON or a wrong shape is an error" in {
    val codec = Codec.fromJson[Order]
    codec.decode("not json".getBytes(UTF_8)).isLeft shouldBe true
    codec.decode("""{"id":1}""".getBytes(UTF_8)).isLeft shouldBe true
  }
}
