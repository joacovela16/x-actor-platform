package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Arithmentics


case object IsFactor {

  def unapply(op: Arithmentics): Option[Arithmentics.Factor] = {
    op match {
      case Arithmentics.Times  => Option(Arithmentics.Times)
      case Arithmentics.Divide => Option(Arithmentics.Divide)
      case _ =>
        None
    }
  }

}
