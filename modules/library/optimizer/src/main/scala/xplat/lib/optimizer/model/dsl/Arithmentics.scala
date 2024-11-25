package xplat.lib.optimizer.model.dsl

sealed trait Arithmentics

object Arithmentics {

  sealed trait Addition extends Arithmentics {

    def sign: Int = this match {
      case Plus  => 1
      case Minus => -1
    }

    def withSign(expr: Expr.Numerical): Expr.Numerical = {
      this match {
        case Plus =>
          expr
        case Minus =>
          -1 * expr
      }
    }

  }

  sealed trait Factor extends Arithmentics
  case object Plus extends Addition
  case object Minus extends Addition
  case object Times extends Factor
  case object Divide extends Factor
}
