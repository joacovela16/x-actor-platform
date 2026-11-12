package xplat.core.api

import scala.concurrent.duration.FiniteDuration

/** Constructors of [[Behavior]]. The effect type and the message type are not written: Scala infers them from the
  * expected type (`ActorSystem#spawn[Msg]`, a method's declared result, the body of another behavior).
  *
  * {{{
  * system.spawn[Counter]("counter")(Behaviors.setup { ctx =>
  *   Behaviors.message { case Increment => Behaviors.same }
  * })
  * }}}
  */
object Behaviors {
  def message[F[_], T](f: T => Behavior[F, T]): Behavior[F, T] = Behavior.OnMessage(f)
  def setup[F[_], T](f: Context[F, T] => Behavior[F, T]): Behavior[F, T] = Behavior.OnContext(f)
  def stash[F[_], T](size: Int)(f: Stash[F, T] => Behavior[F, T]): Behavior[F, T] = Behavior.Stasher(size, f)
  /** `M <:< T` instead of `message: T`: otherwise Scala fixes `T` to the type of `message` (a singleton like `Tick`)
    * after the first parameter list, before looking at the expected type.
    */
  def timer[F[_], T, M](time: FiniteDuration, message: M)(f: Cancelable[F] => Behavior[F, T])(implicit ev: M <:< T): Behavior[F, T] =
    Behavior.Timer(time, ev(message), f)
  def pipe[F[_], T](f: => F[Behavior[F, T]]): Behavior[F, T] = Behavior.Wrapper(f)
  /** Same as `behavior.onSignal(f)`, but the types of `behavior` are inferred from the expected type. Both in one
    * parameter list: in a curried form (or after a `.`) Scala types `behavior` before knowing them.
    */
  def onSignal[F[_], T](behavior: Behavior[F, T], f: PartialFunction[Signal, Behavior[F, T]]): Behavior[F, T] = Behavior.OnSignal(f, behavior)
  def same[F[_], T]: Behavior[F, T] = Behavior.Same()
  def stop[F[_], T]: Behavior[F, T] = Behavior.Stop()
}
