package io.exsql.bytegraph

import com.google.common.primitives.{Ints, Longs, Shorts}
import org.apache.commons.codec.binary.Hex

object ByteArrayUtils {

  def copy(bytes: Array[Byte], position: Int, size: Int): Array[Byte] = {
    val copied = Array.ofDim[Byte](size)
    System.arraycopy(bytes, position, copied, 0, size)

    copied
  }

  def concat(bytes: Array[Byte]*): Array[Byte] = {
    val length = bytes.map(_.length).sum
    val buffer = Array.ofDim[Byte](length)

    var offset = 0
    bytes.foreach { array =>
      System.arraycopy(array, 0, buffer, offset, array.length)
      offset += array.length
    }

    buffer
  }

  def toShort(bytes: Array[Byte], offset: Int): Short = Shorts.fromBytes(bytes(offset), bytes(offset + 1))
  def toInt(bytes: Array[Byte], offset: Int): Int = {
    Ints.fromBytes(bytes(offset), bytes(offset + 1), bytes(offset + 2), bytes(offset + 3))
  }

  def toLong(bytes: Array[Byte], offset: Int): Long = {
    Longs.fromBytes(bytes(offset), bytes(offset + 1), bytes(offset + 2), bytes(offset + 3),
                    bytes(offset + 4), bytes(offset + 5), bytes(offset + 6), bytes(offset + 7))
  }

  def toHexString(bytes: Array[Byte]): String = {
    Hex.encodeHexString(bytes)
  }

}
