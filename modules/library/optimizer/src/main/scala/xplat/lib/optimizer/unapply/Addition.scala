package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Arithmentics


case object Addition {

  def unapply(op: Arithmentics): Option[Arithmentics] = {
    op match {
      case Arithmentics.Plus  => Option(op)
      case Arithmentics.Minus => Option(op)
      case _ =>
        None
    }
  }

}
