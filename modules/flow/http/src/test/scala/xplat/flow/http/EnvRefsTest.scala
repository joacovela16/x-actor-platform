package xplat.flow.http

import xplat.flow.nodes.ConnectionKind
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model.{ConfigSchema, DataType}

class EnvRefsTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(t: String): Json = parse(t).fold(e => throw e, identity)
  private val env: EnvRefs.Env = Map("DB_HOST" -> "db.internal", "DB_PORT" -> "6543", "DB_SSL" -> "true", "DB_PASSWORD" -> "s3cret", "EMPTY" -> "").get

  "text" - {
    "replaces references, uses defaults and keeps $${ as a literal" in IO {
      EnvRefs.resolve("jdbc://${DB_HOST}:${DB_PORT}/${DB_NAME:-orders}", env) shouldBe Right("jdbc://db.internal:6543/orders")
      EnvRefs.resolve("$${DB_HOST} costs $5", env) shouldBe Right("${DB_HOST} costs $5")
      EnvRefs.resolve("${EMPTY:-fallback}", env) shouldBe Right("fallback")
    }

    "says which variables are missing" in IO {
      EnvRefs.resolve("${NOPE}/${DB_HOST}/${ALSO_NOPE}", env) shouldBe Left(List("NOPE", "ALSO_NOPE"))
      EnvRefs.names("${A}-${B:-x}-$${C}") shouldBe List("A", "B")
    }
  }

  "the unified form ${env.NAME} means the same as ${NAME}; other origins are not for connections" in IO {
    EnvRefs.resolve("${env.DB_HOST}:${env.DB_PORT}/${env.DB_NAME:-orders}", env) shouldBe Right("db.internal:6543/orders")
    EnvRefs.names("${env.A}-${B}") shouldBe List("A", "B")
    EnvRefs.isWhole("${env.DB_PORT}") shouldBe true
    EnvRefs.problems(json("""{"host": "${state.domain}", "port": "${env.P}"}""")) shouldBe List("'${state.domain}': a connection can only use env.")
  }

  "a whole reference in a number or boolean field gets that type" in IO {
    val schema = ConfigSchema(List(ConfigSchema.Field("host", DataType.Str), ConfigSchema.Field("port", DataType.Num), ConfigSchema.Field("ssl", DataType.Bool)))
    EnvRefs.resolveParams(json("""{"host": "${DB_HOST}", "port": "${DB_PORT}", "ssl": "${DB_SSL}"}"""), schema, env) shouldBe
      Right(json("""{"host": "db.internal", "port": 6543, "ssl": true}"""))
    schema.validate(EnvRefs.forValidation(json("""{"host": "h", "port": "${DB_PORT}", "ssl": "${DB_SSL}"}"""), schema)) shouldBe Nil
  }

  "connections" - {
    def setup(env: EnvRefs.Env) =
      for {
        secrets <- SecretStore.make[IO]()
        store <- ConnectionStore.inSlot[IO](ConnectionKind.defaults, secrets, None, env)
      } yield (store, secrets)

    "nodes get the values of the environment, also for secrets; the API shows only the names" in {
      for {
        pair <- setup(env)
        (store, secrets) = pair
        saved <- store.put("api", "http", json("""{"baseUrl": "https://${DB_HOST}/v1"}"""), Map("token" -> "${DB_PASSWORD}"))
        resolved <- ConnectionStore.provider[IO](store, secrets, env).resolve("api")
      } yield {
        saved.map(v => (v.params, v.env, v.envMissing)) shouldBe Right((json("""{"baseUrl": "https://${DB_HOST}/v1"}"""), List("DB_HOST", "DB_PASSWORD"), Nil))
        resolved.map(r => (r.params, r.secrets)) shouldBe Some((json("""{"baseUrl": "https://db.internal/v1"}"""), Map("token" -> "s3cret")))
      }
    }

    "a variable that is not set is shown, and a node that uses the connection fails saying which" in {
      val none: EnvRefs.Env = _ => None
      for {
        pair <- setup(none)
        (store, secrets) = pair
        saved <- store.put("api", "http", json("""{"baseUrl": "https://${API_HOST}/${API_VERSION:-v1}"}"""), Map.empty)
        failed <- ConnectionStore.provider[IO](store, secrets, none).resolve("api").attempt
      } yield {
        saved.map(_.envMissing) shouldBe Right(List("API_HOST"))
        failed.left.map(_.getMessage) shouldBe Left("the connection 'api' uses the environment variable 'API_HOST', which is not set")
      }
    }
  }
}
