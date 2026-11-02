package xplat.core

package object api {

  @deprecated("Renamed to ReplyTo, so it does not clash with cats.effect.Ref", "0.4.0")
  type Ref[T] = ReplyTo[T]
}
