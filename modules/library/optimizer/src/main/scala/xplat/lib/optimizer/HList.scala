package xplat.lib.optimizer

sealed trait HList {}

object HList {

  final case class :+:[H, T <: HList](head: H, tail: T) extends HList {
    def :+:[M](value: M): M :+: H :+: T = HList.:+:(value, this)
  }

  trait HNil extends HList

  case object HNil extends HNil {
    def :+:[M](value: M): M :+: HNil = HList.:+:(value, HNil)
  }

}
