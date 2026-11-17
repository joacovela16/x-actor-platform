package xplat.flow.engine

import io.circe.Encoder

/** A part of a longer list, and where the next part starts (`next` is empty at the end). */
final case class Page[A](items: List[A], next: Option[String])

object Page {
  implicit def encoder[A: Encoder]: Encoder[Page[A]] = Encoder.forProduct2("items", "next")((p: Page[A]) => (p.items, p.next))
}
