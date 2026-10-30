package xplat.core.io.sharding

import xplat.core.api.Codec
import xplat.core.api.error.CodecError
import xplat.core.cluster.api.Address
import xplat.core.io.net.{BinReader, BinWriter}

import scala.util.Try

/** Messages between shard regions and the coordinator of one entity type. */
private[sharding] object ShardingProtocol {

  /** Messages for the coordinator. */
  sealed trait CoordinatorMsg

  /** Sent periodically by every region that can host shards: the shards it hosts and their running entities. */
  final case class Register(region: Address, shards: Map[String, Set[String]]) extends CoordinatorMsg
  final case class GetShardHome(shardId: String, region: Address) extends CoordinatorMsg

  /** A region finished stopping a shard (after a hand-off or a lost lease). */
  final case class ShardStopped(shardId: String, region: Address, entities: Set[String]) extends CoordinatorMsg

  /** A region is shutting down and hosts nothing any more. */
  final case class RegionStopped(region: Address) extends CoordinatorMsg
  case object Tick extends CoordinatorMsg

  /** Messages for a region. */
  sealed trait RegionMsg

  /** Where a shard lives. The home region also starts `entities` (remembered entities). */
  final case class ShardHome(shardId: String, home: Address, entities: Set[String]) extends RegionMsg
  final case class HandOff(shardId: String) extends RegionMsg

  /** A message for an entity forwarded by another region. */
  final case class EntityMessage(entityId: String, payload: Array[Byte]) extends RegionMsg

  private def address(w: BinWriter, a: Address): Unit = { w.string(a.system).string(a.host).int(a.port); () }
  private def address(r: BinReader): Address = Address(r.string(), r.string(), r.int())
  private def ids(w: BinWriter, s: Set[String]): Unit = { w.seq(s)(_.string(_)); () }
  private def ids(r: BinReader): Set[String] = r.seq(_.string()).toSet

  private def attempt[A](f: => A): Either[CodecError, A] = Try(f).toEither.left.map(e => CodecError(s"cannot decode sharding message: $e"))

  val coordinatorCodec: Codec[CoordinatorMsg, Array[Byte]] = new Codec[CoordinatorMsg, Array[Byte]] {
    def encode(from: CoordinatorMsg): Either[CodecError, Array[Byte]] = {
      val w = BinWriter()
      from match {
        case Register(region, shards) => w.byte(1); address(w, region); w.seq(shards) { case (w, (s, e)) => w.string(s); ids(w, e) }
        case GetShardHome(shard, region) => w.byte(2).string(shard); address(w, region)
        case ShardStopped(shard, region, entities) => w.byte(3).string(shard); address(w, region); ids(w, entities)
        case RegionStopped(region) => w.byte(4); address(w, region)
        case Tick => w.byte(5)
      }
      Right(w.result())
    }

    def decode(to: Array[Byte]): Either[CodecError, CoordinatorMsg] = attempt {
      val r = BinReader(to)
      r.byte() match {
        case 1 => Register(address(r), r.seq(r => r.string() -> ids(r)).toMap)
        case 2 => GetShardHome(r.string(), address(r))
        case 3 => ShardStopped(r.string(), address(r), ids(r))
        case 4 => RegionStopped(address(r))
        case 5 => Tick
      }
    }
  }

  val regionCodec: Codec[RegionMsg, Array[Byte]] = new Codec[RegionMsg, Array[Byte]] {
    def encode(from: RegionMsg): Either[CodecError, Array[Byte]] = {
      val w = BinWriter()
      from match {
        case ShardHome(shard, home, entities) => w.byte(1).string(shard); address(w, home); ids(w, entities)
        case HandOff(shard)                   => w.byte(2).string(shard)
        case EntityMessage(entity, payload)   => w.byte(3).string(entity).bytes(payload)
      }
      Right(w.result())
    }

    def decode(to: Array[Byte]): Either[CodecError, RegionMsg] = attempt {
      val r = BinReader(to)
      r.byte() match {
        case 1 => ShardHome(r.string(), address(r), ids(r))
        case 2 => HandOff(r.string())
        case 3 => EntityMessage(r.string(), r.bytes())
      }
    }
  }
}
