package xplat.core.io.net

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

/** Minimal binary encoding for internal protocols. Fields are written and read back in the same order. */
final class BinWriter private () {
  private val buffer = new ByteArrayOutputStream()
  private val out = new DataOutputStream(buffer)

  def byte(v: Int): BinWriter = { out.writeByte(v); this }
  def bool(v: Boolean): BinWriter = { out.writeBoolean(v); this }
  def int(v: Int): BinWriter = { out.writeInt(v); this }
  def long(v: Long): BinWriter = { out.writeLong(v); this }

  def string(v: String): BinWriter = bytes(v.getBytes(UTF_8))

  def bytes(v: Array[Byte]): BinWriter = { out.writeInt(v.length); out.write(v); this }

  def option[A](v: Option[A])(f: (BinWriter, A) => Unit): BinWriter = {
    bool(v.isDefined)
    v.foreach(f(this, _))
    this
  }

  def seq[A](v: Iterable[A])(f: (BinWriter, A) => Unit): BinWriter = {
    int(v.size)
    v.foreach(f(this, _))
    this
  }

  def result(): Array[Byte] = { out.flush(); buffer.toByteArray }
}

object BinWriter {
  def apply(): BinWriter = new BinWriter()
}

final class BinReader private (data: Array[Byte]) {
  private val in = new DataInputStream(new ByteArrayInputStream(data))

  def byte(): Int = in.readByte().toInt
  def bool(): Boolean = in.readBoolean()
  def int(): Int = in.readInt()
  def long(): Long = in.readLong()

  def string(): String = new String(bytes(), UTF_8)

  def bytes(): Array[Byte] = {
    val size = in.readInt()
    require(size >= 0 && size <= in.available(), s"invalid length $size")
    val v = new Array[Byte](size)
    in.readFully(v)
    v
  }

  def option[A](f: BinReader => A): Option[A] = if (bool()) Some(f(this)) else None

  def seq[A](f: BinReader => A): Vector[A] = {
    val size = int()
    require(size >= 0, s"invalid size $size")
    Vector.fill(size)(f(this))
  }
}

object BinReader {
  def apply(data: Array[Byte]): BinReader = new BinReader(data)
}
