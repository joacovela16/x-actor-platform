package xplat.core.sharding

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import xplat.core.api.{Behaviors, Codec}
import xplat.core.api.error.CodecError
import xplat.core.sharding.api._

import java.nio.charset.StandardCharsets.UTF_8

class ShardingApiTest extends AnyFunSuite with Matchers {

  private type F[A] = Either[Throwable, A]

  implicit private val stringCodec: Codec[String, Array[Byte]] = new Codec[String, Array[Byte]] {
    def encode(from: String): Either[CodecError, Array[Byte]] = Right(from.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, String] = Right(new String(to, UTF_8))
  }

  test("hash extractor always maps an entity to the same shard") {
    val extractor = ShardIdExtractor.hashCode(10)
    extractor.shardId("account-1") shouldBe extractor.shardId("account-1")
  }

  test("hash extractor keeps shard ids within range, also for negative hash codes") {
    val extractor = ShardIdExtractor.hashCode(7)
    val ids = Seq("", "a", "account-1", "polygenelubricants", "x" * 100) // "polygenelubricants".hashCode == Int.MinValue
    all(ids.map(id => extractor.shardId(id).toInt)) should (be >= 0 and be < 7)
  }

  test("hash extractor spreads entities over all shards") {
    val extractor = ShardIdExtractor.hashCode(10)
    (1 to 1000).map(i => extractor.shardId(s"entity-$i")).toSet.size shouldBe 10
  }

  test("hash extractor rejects a non positive number of shards") {
    an[IllegalArgumentException] should be thrownBy ShardIdExtractor.hashCode(0)
  }

  test("an entity uses defaults that can be overridden") {
    val key = EntityTypeKey[String]("greeter")
    val entity = Entity[F, String](key)(_ => (b: Behaviors[F, String]) => b.same)

    entity.config shouldBe ShardingConfig()
    entity.stopMessage shouldBe None
    entity.allocationStrategy shouldBe None
    entity.withStopMessage("stop").withConfig(ShardingConfig(role = Some("backend"))).stopMessage shouldBe Some("stop")
    entity.codec shouldBe stringCodec
  }

  test("region state lists the ids of all local entities") {
    ShardRegionState(Map("1" -> Set("a", "b"), "2" -> Set("c"))).entityIds shouldBe Set("a", "b", "c")
  }
}
