package xplat.core.persistence

import xplat.core.api.{Behavior, Codec}
import xplat.core.persistence.api.{Effects, Journal, JournalBuilder}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait Persistence[F[_]] {

  def build[State, Command, Event](
      name: String,
      initialState: State,
      commandHandler: (State, Command) => F[Effects[Event]],
      eventHandler: (State, Event) => State,
      config: Persistence.Config = Persistence.Config()
  )(implicit stateCodec: Codec[State, Array[Byte]], eventCode: Codec[Event, Array[Byte]]): Behavior[F, Command]

}

object Persistence {

  final case class Config(shutdownOnIdle: Option[FiniteDuration] = Some(1.minute))

  def apply[F[_], State, Command, Event](
      name: String,
      initialState: State,
      commandHandler: (State, Command) => F[Effects[Event]],
      eventHandler: (State, Event) => State
  )(implicit
      p: Persistence[F],
      stateCodec: Codec[State, Array[Byte]],
      eventCode: Codec[Event, Array[Byte]]
  ): Behavior[F, Command] = p.build(
    name,
    initialState,
    commandHandler,
    eventHandler
  )

}
