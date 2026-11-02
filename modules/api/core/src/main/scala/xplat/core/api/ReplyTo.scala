package xplat.core.api

/** Where to send the answer to a message, as used by `ask`. It is one-way: `send` delivers the answer and returns at once. */
trait ReplyTo[T] {
  def send(msg: T): Unit
  def !(msg: T): Unit = send(msg)
}
