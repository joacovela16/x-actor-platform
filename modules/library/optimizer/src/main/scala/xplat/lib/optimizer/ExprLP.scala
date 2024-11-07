package xplat.lib.optimizer

import scala.language.implicitConversions

trait ExprLP {

  implicit def mkExpr[T](value: T): Expr[T] = ???
}
