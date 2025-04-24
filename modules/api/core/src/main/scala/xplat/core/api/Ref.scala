package xplat.core.api

trait Ref[T] {
  def send(msg: T): Unit
  def !(msg: T): Unit = send(msg)
}
