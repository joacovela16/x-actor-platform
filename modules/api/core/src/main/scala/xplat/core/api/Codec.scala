package xplat.core.api

import xplat.core.api.error.CodecError

trait Codec[From, To] {
  def encode(from: From): Either[CodecError, To]

  def decode(to: To): Either[CodecError, From]
}
