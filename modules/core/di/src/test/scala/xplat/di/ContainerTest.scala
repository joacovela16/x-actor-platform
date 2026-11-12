package xplat.di

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref, Resource}
import cats.implicits._
import xplat.di.Key.declare
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class ContainerTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  final case class Logger(name: String)
  final case class Tool(name: String)
  final case class Agent(log: Logger, tools: Many[IO, Tool])

  private val Log = Key.single[Logger]("logger")
  private val Tools = Key.multiple[Tool]("tools")
  private val AgentKey = Key.single[Agent]("agent")
  private val Rows = Key.multiple[String]("rows")
  private val PaysCaja = Key.single[Unit]("pays-caja")

  /** A resource that records when it starts and stops. */
  private def tracked[A](log: Ref[IO, List[String]], name: String, value: A): Resource[IO, A] =
    Resource.make(log.update(_ :+ s"start $name").as(value))(_ => log.update(_ :+ s"stop $name"))

  "provide" - {

    "registers services and resolves their dependencies, typed" in {
      Container[IO].use { c =>
        for {
          _     <- c.provide(
                     Log.pure(Logger("console")),
                     Tools.dependsOn(Log).pure(log => Tool(s"read:${log.name}")),
                     Tools.pure(Tool("write")),
                     AgentKey.dependsOn(Log, Tools).pure((log, tools) => Agent(log, tools))
                   )
          agent <- c.get(AgentKey)
          tools <- agent.tools.current
        } yield (agent.log, tools) shouldBe (Logger("console"), List(Tool("read:console"), Tool("write")))
      }
    }

    "builds each service once" in {
      Container[IO].use { c =>
        for {
          count <- Ref[IO].of(0)
          _     <- c.provide(Log.eval(count.update(_ + 1).as(Logger("x"))))
          _     <- c.get(Log) >> c.get(Log)
          n     <- count.get
        } yield n shouldBe 1
      }
    }

    "the returned registration removes all its services" in {
      Container[IO].use { c =>
        for {
          reg    <- c.provide(Log.pure(Logger("x")), Tools.pure(Tool("a")))
          tools  <- c.get(Tools)
          _      <- reg.remove
          has    <- c.has(Log)
          after  <- tools.current
          keys   <- c.registeredKeys
        } yield (has, after, keys) shouldBe (false, Nil, Nil)
      }
    }

    "if one fails, none stays registered" in {
      Container[IO].use { c =>
        for {
          _      <- c.provide(Log.pure(Logger("x")))
          result <- c.provide(Tools.pure(Tool("a")), Log.pure(Logger("again"))).attempt
          keys   <- c.registeredKeys
        } yield {
          result.left.map(_.getMessage) shouldBe Left("Key(logger) already has a service")
          keys shouldBe List(Log)
        }
      }
    }

    "a missing service names who required it" in {
      Container[IO].use { c =>
        c.provide(AgentKey.dependsOn(Log, Tools).pure(Agent(_, _))) >>
          (c.get(AgentKey).attempt, c.missing(AgentKey)).tupled.map { case (result, missing) =>
            result.left.map(_.getMessage) shouldBe Left("No service for Key(logger) (required by Key(agent))")
            missing shouldBe List(Log)
          }
      }
    }

    "circular dependencies fail" in {
      val A = Key.single[String]("a")
      val B = Key.single[String]("b")
      Container[IO].use { c =>
        c.provide(A.dependsOn(B).pure(identity), B.dependsOn(A).pure(identity)) >>
          c.get(A).attempt.map(_.left.map(_.getMessage) shouldBe Left("Circular dependency: a -> b -> a"))
      }
    }
  }

  "resources" - {

    "start when resolved and stop when their registration is removed" in {
      for {
        log    <- Ref[IO].of(List.empty[String])
        result <- Container[IO].use { c =>
                    for {
                      reg    <- c.provide(Log.resource(tracked(log, "logger", Logger("x"))))
                      _      <- c.get(Log)
                      _      <- reg.remove
                      events <- log.get
                    } yield events
                  }
      } yield result shouldBe List("start logger", "stop logger")
    }

    "removing a dependency stops its dependents, which are built again on the next request" in {
      for {
        log    <- Ref[IO].of(List.empty[String])
        events <- Container[IO].use { c =>
                    for {
                      reg <- c.provide(Log.resource(tracked(log, "logger", Logger("x"))))
                      _   <- c.provide(AgentKey.dependsOn(Log, Tools).resource((l, t) => tracked(log, "agent", Agent(l, t))))
                      _   <- c.get(AgentKey)
                      _   <- reg.remove
                      _   <- c.provide(Log.pure(Logger("y")))
                      a   <- c.get(AgentKey)
                      _   <- log.update(_ :+ s"agent uses ${a.log.name}")
                    } yield ()
                  } >> log.get
      } yield events shouldBe List("start logger", "start agent", "stop agent", "stop logger", "start agent", "agent uses y", "stop agent")
    }

    "a service added to a resolved multiple key starts at once; it waits for its dependencies" in {
      for {
        log    <- Ref[IO].of(List.empty[String])
        events <- Container[IO].use { c =>
                    for {
                      tools <- c.get(Tools)
                      _     <- c.provide(Tools.dependsOn(Log).resource(l => tracked(log, "tool", Tool(l.name))))
                      none  <- tools.current
                      _     <- log.update(_ :+ s"tools: ${none.size}")
                      reg   <- c.provide(Log.pure(Logger("x")))
                      some  <- tools.current
                      _     <- log.update(_ :+ s"tools: ${some.size}")
                      _     <- reg.remove
                      gone  <- tools.current
                      _     <- log.update(_ :+ s"tools: ${gone.size}")
                    } yield ()
                  } >> log.get
      } yield events shouldBe List("tools: 0", "start tool", "tools: 1", "stop tool", "tools: 0")
    }

    "releasing the container stops everything in reverse order, even if one fails" in {
      for {
        log    <- Ref[IO].of(List.empty[String])
        errors <- Ref[IO].of(List.empty[String])
        options = Container.Options[IO](onReleaseError = Some((k, _) => errors.update(_ :+ k.name)))
        _ <- Container.withOptions(options).use { c =>
               c.provide(
                 Log.resource(tracked(log, "logger", Logger("x"))),
                 Tools.dependsOn(Log).resource(_ => Resource.make(IO.pure(Tool("t")))(_ => IO.raiseError(new RuntimeException("boom")))),
                 AgentKey.dependsOn(Log, Tools).resource((l, t) => tracked(log, "agent", Agent(l, t)))
               ) >> c.get(AgentKey) >> c.get(Tools).flatMap(_.current)
             }
        events <- log.get
        failed <- errors.get
      } yield (events, failed) shouldBe (List("start logger", "start agent", "stop agent", "stop logger"), List("tools"))
    }

    "onBuild reports each service after its dependencies" in {
      for {
        seen <- Ref[IO].of(List.empty[String])
        _ <- Container.withOptions(Container.Options[IO](onBuild = Some(k => seen.update(_ :+ k.name)))).use { c =>
               c.provide(Log.pure(Logger("x")), AgentKey.dependsOn(Log, Tools).pure(Agent(_, _))) >> c.get(AgentKey)
             }
        order <- seen.get
      } yield order shouldBe List("logger", "agent")
    }
  }

  "listeners" - {
    "are told of each registration and removal, and may use the container" in {
      Container[IO].use { c =>
        for {
          seen <- Ref[IO].of(List.empty[String])
          _    <- c.onChange(k => c.has(k).flatMap(h => seen.update(_ :+ s"${k.name}:$h")))
          reg  <- c.provide(Log.pure(Logger("x")))
          _    <- reg.remove
          out  <- seen.get
        } yield out shouldBe List("logger:true", "logger:false")
      }
    }
  }

  "scopes" - {

    "a parent's contribution is built in the scope when the scope has its evidence" in {
      Container[IO].use { app =>
        for {
          _       <- app.provide(Rows.dependsOn(PaysCaja).pure(_ => "caja"), Rows.pure("always"))
          inScope <- app.scope.use(s => s.provide(PaysCaja.pure(())) >> s.get(Rows).flatMap(_.current))
          inApp   <- app.get(Rows).flatMap(_.current)
          missing <- app.missing(PaysCaja)
        } yield (inScope, inApp, missing) shouldBe (List("caja", "always"), List("always"), List(PaysCaja))
      }
    }

    "single keys of the parent are shared; those of the scope do not reach the parent" in {
      Container[IO].use { app =>
        for {
          count <- Ref[IO].of(0)
          _     <- app.provide(Log.eval(count.update(_ + 1).as(Logger("x"))))
          _     <- app.get(Log)
          r     <- app.scope.use(s => s.provide(PaysCaja.pure(())) >> (s.get(Log), app.has(PaysCaja)).tupled)
          n     <- count.get
        } yield (r._1, r._2, n) shouldBe (Logger("x"), false, 1)
      }
    }

    "what the scope registers goes after what it inherits, and it follows the parent's changes" in {
      Container[IO].use { app =>
        app.scope.use { s =>
          for {
            seen  <- Ref[IO].of(List.empty[String])
            _     <- s.onChange(k => seen.update(_ :+ k.name))
            _     <- app.provide(Rows.pure("parent"))
            _     <- s.provide(Rows.pure("scope"))
            rows  <- s.get(Rows)
            both  <- rows.current
            reg   <- app.provide(Rows.pure("later"))
            three <- rows.current
            _     <- reg.remove
            back  <- rows.current
            names <- seen.get
          } yield {
            (both, three, back) shouldBe (List("parent", "scope"), List("parent", "later", "scope"), List("parent", "scope"))
            names shouldBe List("rows", "rows", "rows", "rows")
          }
        }
      }
    }

    "releasing a scope stops what it built and leaves the parent alone" in {
      for {
        log    <- Ref[IO].of(List.empty[String])
        result <- Container[IO].use { app =>
                    for {
                      _      <- app.provide(Log.resource(tracked(log, "logger", Logger("x"))))
                      _      <- app.provide(Rows.dependsOn(Log).resource(l => tracked(log, "row", l.name)))
                      _      <- app.scope.use(_.get(Rows).flatMap(_.current))
                      events <- log.get
                      still  <- app.get(Log)
                    } yield (events, still)
                  }
      } yield result shouldBe (List("start logger", "start row", "stop row"), Logger("x"))
    }

    "a scope of a scope inherits from both" in {
      Container[IO].use { app =>
        app.provide(Rows.dependsOn(PaysCaja).pure(_ => "caja")) >>
          app.scope.use { month =>
            month.provide(PaysCaja.pure(()), Rows.pure("month")) >>
              month.scope.use(day => day.provide(Rows.pure("day")) >> day.get(Rows).flatMap(_.current))
          }
      }.map(_ shouldBe List("caja", "month", "day"))
    }
  }
}
