package xplat.flow.server

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.concurrent.duration._

class ServerConfigTest extends AnyFreeSpec with Matchers {

  "defaults" in {
    ServerConfig.parse(Nil, Map.empty) shouldBe Right(ServerConfig())
  }

  "the environment sets it and the arguments win" in {
    val env = Map("FLOW_PORT" -> "9000", "FLOW_DATA_DIR" -> "/data", "FLOW_DB" -> "memory", "FLOW_NODE_TIMEOUT_SECONDS" -> "30", "FLOW_HOST" -> "127.0.0.1")
    ServerConfig.parse(Nil, env) shouldBe Right(ServerConfig("127.0.0.1", 9000, Paths.get("/data"), ServerConfig.Database.Memory, 30.seconds))
    ServerConfig.parse(List("--port", "9100", "--db", "sqlite"), env).map(c => (c.port, c.database)) shouldBe Right((9100, ServerConfig.Database.Sqlite))
  }

  "bad values are reported" in {
    ServerConfig.parse(List("--port", "x"), Map.empty).isLeft shouldBe true
    ServerConfig.parse(List("--db", "oracle"), Map.empty).isLeft shouldBe true
    ServerConfig.parse(List("--what", "1"), Map.empty).isLeft shouldBe true
    ServerConfig.parse(List("--port"), Map.empty).isLeft shouldBe true
    ServerConfig.parse(Nil, Map("FLOW_NODE_TIMEOUT_SECONDS" -> "0")).isLeft shouldBe true
  }
}
