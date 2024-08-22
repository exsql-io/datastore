package io.exsql.bytegraph

import net.openhft.hashing.LongHashFunction

object HashingUtils {

  def hash(bytes: Array[Byte], position: Int, length: Int): Long = {
    LongHashFunction.xx().hashBytes(bytes, position, length)
  }

  def hashInt(bytes: Array[Byte], position: Int, length: Int): Int = hash(bytes, position, length).toInt

}
