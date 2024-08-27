package xplat.core.io.actor

import cats.effect.Async
import cats.implicits._
import xplat.core.api._
import xplat.core.api.error.{CodecError, XError}
import xplat.core.persistence.Persistence
import xplat.core.persistence.api.journal.JournalEvent
import xplat.core.persistence.api.{Effect, Effects, Journal, JournalBuilder}

import scala.concurrent.duration.FiniteDuration

object IOPersistence {
  private type Mapper[From] = Codec[From, Array[Byte]]

  implicit def IOPersistence[F[_]](implicit M: Async[F], journalBuilder: JournalBuilder[F]): Persistence[F] = new Persistence[F] {

    override def build[State, Command, Event](
        name: String,
        initialState: State,
        commandHandler: (State, Command) => F[Effects[Event]],
        eventHandler: (State, Event) => State,
        config: Persistence.Config
    )(implicit stateCodec: Mapper[State], eventCodec: Mapper[Event]): Behaviors[F, Command] => Behavior[F, Command] = { b =>
      b.context { ctx =>
        val shard: Journal.Shard = Journal.Shard(name, ctx.name)

        def parsingResult[T](result: Either[CodecError, T]): F[T] = {
          result match {
            case Left(value) =>
              M.raiseError(XError(value.message))
            case Right(value) =>
              M.pure(value)
          }
        }

        def foldEffects(state: State, command: Command, stash: Stash[F, Command], entries: Seq[Effect[Event]], journalQuery: Journal[F]): F[State] = entries match {
          case Nil =>
            state.pure[F]
          case head +: tail =>
            head match {
              case Effect.Stash =>
                stash.push(command) >> foldEffects(state, command, stash, tail, journalQuery)
              case Effect.Unstash =>
                stash.unstashAll() >> foldEffects(state, command, stash, tail, journalQuery)
              case Effect.Ignore =>
                state.pure[F]
              case Effect.Persist(event) =>
                for {
                  nextState <- M.pure(eventHandler(state, event))
                  encodedEvents <- Seq(event).traverse(e => parsingResult(eventCodec.encode(e)))
                  _ <- journalQuery.writeEvents(shard, encodedEvents)
                  next <- foldEffects(nextState, command, stash, tail, journalQuery)
                } yield next
              case Effect.Compress =>
                for {
                  encoded <- parsingResult(stateCodec.encode(state))
                  _ <- journalQuery.writeSnapshot(shard, encoded)
                  next <- foldEffects(state, command, stash, tail, journalQuery)
                } yield next
            }
        }

        def startAliveDaemon(state: State, stash: Stash[F, Command], journalQuery: Journal[F]): Behavior[F, Command] = {
          b.message { cmd =>
            for {
              effects <- commandHandler(state, cmd)
              next <- foldEffects(state, cmd, stash, effects.entries, journalQuery)
            } yield startAliveDaemon(next, stash, journalQuery)
          }
        }

        def startLazyDaemon(state: State, stash: Stash[F, Command], journalQuery: Journal[F], timeout: FiniteDuration): Behavior[F, Command] = {
          b.context { ctx =>
            for {
              cancellable <- ctx.schedule(
                M.pure(ctx.signal(Signal.Kill)) >> ctx.log.info(
                  s"Shutting down persistence actor " +
                    s"'${ctx.name}'"
                ),
                timeout
              )
            } yield {
              b.message { cmd =>
                for {
                  effects <- commandHandler(state, cmd)
                  _ <- cancellable()
                  next <- foldEffects(state, cmd, stash, effects.entries, journalQuery)
                } yield startLazyDaemon(next, stash, journalQuery, timeout)
              }
            }
          }
        }

        def daemon(state: State, stash: Stash[F, Command], journal: Journal[F]): Behavior[F, Command] = {
          config.shutdownOnIdle match {
            case Some(timeout) =>
              startLazyDaemon(state, stash, journal, timeout)
            case None =>
              startAliveDaemon(state, stash, journal)
          }
        }

        def recoverState(id: String, initialState: State): Behavior[F, Command] = {

          def fold(entries: LazyList[JournalEvent], state: State): F[State] = {
            entries.foldLeftM[F, State](state) { case (currentState, event) =>
              parsingResult(eventCodec.decode(event.data)).map(value => eventHandler(currentState, value))
            }
          }

          for {
            journal <- journalBuilder.build()
            _ <- journal.create(shard)
            eventualState <- journal.getSnapshot(shard)
            maybeState = eventualState.flatMap(_.data).map(x => parsingResult(stateCodec.decode(x)))
            entries <- journal.getEvents(shard)
            initialState <- maybeState.getOrElse(M.pure(initialState))
            currentState <- fold(entries, initialState)
          } yield {
            b
              .stash(0)(stash => daemon(currentState, stash, journal))
              .onSignal { case Signal.PostStop =>
                journal.close() >> ctx.log.info("2shutting down") >> M.pure(b.same)
              }
          }
        }

        recoverState(ctx.name, initialState)

      }
    }

  }

}
