package xplat.lib.optimizer.model.dsl

sealed trait Arithmentics

object Arithmentics {
  sealed trait Addition extends Arithmentics
  sealed trait Factor extends Arithmentics
  case object Plus extends Addition
  case object Minus extends Addition
  case object Times extends Factor
  case object Divide extends Factor
}
