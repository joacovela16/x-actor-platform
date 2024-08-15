package xplat.core.api

trait Cancelable[F[_]] extends (() => F[Unit])
