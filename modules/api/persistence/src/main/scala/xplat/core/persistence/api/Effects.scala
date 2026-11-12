package xplat.core.persistence.api

import com.softwaremill.quicklens._

import scala.language.implicitConversions

final case class Effects[Event](entries: Seq[Effect[Event]]) {
  def stash: Effects[Event] = this.modify(_.entries).using(_ :+ Effect.Stash)

  def unstash: Effects[Event] = this.modify(_.entries).using(_ :+ Effect.Unstash)

  def persist(value: Event): Effects[Event] = this.modify(_.entries).using(_ :+ Effect.Persist(value))

  def compress: Effects[Event] = this.modify(_.entries).using(_ :+ Effect.Compress)

  def ignore: Effects[Event] = this.modify(_.entries).using(_ :+ Effect.Ignore)
}

object Effects {
  def apply[Event]: Effects[Event] = Effects(Nil)

  implicit def mkFromEffect[A](effect: Effect[A]): Effects[A] = Effects(Seq(effect))
}
