package xplat.flow.http

import xplat.flow.nodes.ConnectionKind
import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._
import io.circe.{Decoder, Encoder, Json}

import java.nio.file.Path

/** The named connections: global configuration that nodes use by name. What is not secret is kept here; the values of the
  * secret fields are kept in the [[SecretStore]], and a connection only says which of them are set. Parameters and secrets can refer
  * to environment variables (see [[EnvRefs]]); they are resolved when a node asks for the connection.
  */
trait ConnectionStore[F[_]] {

  def kinds: List[ConnectionKind]

  def list: F[List[ConnectionStore.View]]

  def get(name: String): F[Option[ConnectionStore.View]]

  /** Creates or updates a connection. A secret that is not given keeps its value, so a form that does not show secrets can be
    * saved; a secret given empty is not changed either. It is refused, with the reasons, if the kind does not exist or the
    * configuration does not fit it.
    */
  def put(name: String, kind: String, params: Json, secrets: Map[String, String]): F[Either[List[String], ConnectionStore.View]]

  def delete(name: String): F[Boolean]
}

object ConnectionStore {

  /** A connection as the API shows it. `secretsSet` has the names of the secret fields that have a value. `env` has the environment
    * variables that its parameters and secrets refer to, and `envMissing` the ones of them that are not set and have no default.
    */
  final case class View(name: String, kind: String, params: Json, secretsSet: List[String], env: List[String] = Nil, envMissing: List[String] = Nil)

  object View {
    implicit val encoder: Encoder[View] =
      Encoder.forProduct6("name", "kind", "params", "secretsSet", "env", "envMissing")((v: View) => (v.name, v.kind, v.params, v.secretsSet, v.env, v.envMissing))
  }

  private final case class Stored(kind: String, params: Json)
  private object Stored {
    implicit val codec: io.circe.Codec[Stored] = io.circe.Codec.forProduct2("kind", "params")(Stored.apply)((s: Stored) => (s.kind, s.params))
  }

  /** The connections as the nodes see them (see [[xplat.flow.nodes.Connections]]): with their secrets, which only a node that
    * connects may have.
    */
  def provider[F[_]](store: ConnectionStore[F], secrets: SecretStore[F], env: EnvRefs.Env = EnvRefs.system)(implicit
      F: cats.MonadThrow[F]
  ): xplat.flow.nodes.Connections[F] =
    (name: String) =>
      store.get(name).flatMap {
        case None => F.pure(None)
        case Some(view) =>
          val schema = store.kinds.find(_.kind == view.kind).map(_.config).getOrElse(xplat.flow.model.ConfigSchema(Nil))
          view.secretsSet
            .traverse(field => secrets.get(s"$name.$field").map(_.map(field -> _)))
            .flatMap { found =>
              val plain = found.flatten
              val resolved = for {
                params <- EnvRefs.resolveParams(view.params, schema, env)
                values <- plain.traverse { case (f, v) => EnvRefs.resolve(v, env).map(f -> _) }.leftMap(identity)
              } yield xplat.flow.nodes.Connections.Resolved(name, view.kind, params, values.toMap)
              resolved match {
                case Right(r) => F.pure(Some(r))
                case Left(missing) =>
                  F.raiseError(new IllegalStateException(s"the connection '$name' uses the environment variable${if (missing.size > 1) "s" else ""} ${missing.map(m => s"'$m'").mkString(", ")}, which ${if (missing.size > 1) "are" else "is"} not set"))
              }
            }
      }

  def make[F[_]: Async](kinds: List[ConnectionKind], secrets: SecretStore[F], file: Option[Path] = None): F[ConnectionStore[F]] =
    inSlot(kinds, secrets, file.map(JsonSlot.file[F](_)))

  /** The same, kept in any [[JsonSlot]]. `onChange` is told the name of a connection that was edited or deleted (the server closes
    * its clients).
    */
  def inSlot[F[_]: Async](
      kinds: List[ConnectionKind],
      secrets: SecretStore[F],
      slot: Option[JsonSlot[F]],
      env: EnvRefs.Env = EnvRefs.system,
      onChange: Option[String => F[Unit]] = None
  ): F[ConnectionStore[F]] = {
    val kindList = kinds
    for {
      initial <- slot.fold(Sync[F].pure(Map.empty[String, Stored]))(_.read[Map[String, Stored]](Map.empty))
      ref <- Ref.of[F, Map[String, Stored]](initial)
      lock <- Semaphore[F](1)
    } yield new ConnectionStore[F] {

      val kinds: List[ConnectionKind] = kindList

      private def persist(all: Map[String, Stored]): F[Unit] = slot.traverse_(_.write[Map[String, Stored]](all))

      private def view(name: String, stored: Stored): F[View] =
        secrets.names(s"$name.").flatMap { names =>
          // The references of the secrets count too; their values are not shown, only the names of the variables.
          names.traverse(secrets.get).map { found =>
            val values = found.flatten
            val refs = (EnvRefs.namesIn(stored.params) ++ values.flatMap(EnvRefs.names)).distinct.sorted
            val missing = (EnvRefs.resolveParams(stored.params, xplat.flow.model.ConfigSchema(Nil), env).left.toOption.toList.flatten ++
              values.flatMap(v => EnvRefs.resolve(v, env).left.toOption.toList.flatten)).distinct.sorted
            View(name, stored.kind, stored.params, names.map(_.stripPrefix(s"$name.")), refs, missing)
          }
        }

      def list: F[List[View]] = ref.get.flatMap(_.toList.sortBy(_._1).traverse { case (n, s) => view(n, s) })

      def get(name: String): F[Option[View]] = ref.get.flatMap(_.get(name).traverse(view(name, _)))

      def put(name: String, kind: String, params: Json, provided: Map[String, String]): F[Either[List[String], View]] =
        lock.permit.use { _ =>
          kindList.find(_.kind == kind) match {
            case None => Sync[F].pure(Left(List(s"unknown connection kind '$kind'")))
            case Some(k) =>
              val secretFields = k.config.fields.filter(_.secret).map(_.name).toSet
              val plain = k.config.copy(fields = k.config.fields.filterNot(_.secret))
              val sent = provided.filter { case (field, value) => value.nonEmpty && secretFields(field) }
              val unknownSecrets = provided.keys.filterNot(secretFields).toList.sorted.map(s => s"'$s' is not a secret field of '$kind'")
              secrets.names(s"$name.").map(_.map(_.stripPrefix(s"$name."))).flatMap { already =>
                val missing = k.config.fields.filter(f => f.secret && f.required && f.default.isEmpty).map(_.name).filterNot(f => sent.contains(f) || already.contains(f))
                val refProblems = EnvRefs.problems(params) ++ sent.values.toList.flatMap(v => EnvRefs.problems(Json.fromString(v)))
                val problems = refProblems ++ plain.validate(EnvRefs.forValidation(params, plain)) ++ unknownSecrets ++ missing.map(f => s"missing secret '$f'")
                if (problems.nonEmpty) Sync[F].pure(Left(problems))
                else {
                  val stored = Stored(kind, if (params.isNull) Json.obj() else params)
                  for {
                    all <- ref.get
                    // A connection that changes its kind does not keep the secrets of the old one.
                    _ <- if (all.get(name).exists(_.kind != kind)) secrets.deleteAll(s"$name.") else Sync[F].unit
                    _ <- sent.toList.traverse_ { case (f, v) => secrets.set(s"$name.$f", v) }
                    next = all.updated(name, stored)
                    _ <- persist(next)
                    _ <- ref.set(next)
                    _ <- onChange.traverse_(_(name))
                    v <- view(name, stored)
                  } yield Right(v)
                }
              }
          }
        }

      def delete(name: String): F[Boolean] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            if (all.contains(name)) { val next = all - name; persist(next) >> secrets.deleteAll(s"$name.") >> ref.set(next) >> onChange.traverse_(_(name)).as(true) }
            else Sync[F].pure(false)
          }
        }
    }
  }
}
