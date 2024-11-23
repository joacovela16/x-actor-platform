package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Arithmentics


case object IsAddition {

  def unapply(op: Arithmentics): Option[Arithmentics.Addition] = {
    op match {
      case Arithmentics.Plus  => Option(Arithmentics.Plus)
      case Arithmentics.Minus => Option(Arithmentics.Minus)
      case _ =>
        None
    }

  }

}
