package io.exsql.bytegraph.bytes

import io.exsql.bytegraph.ByteArrayUtils

class ByteGraphByteArrayAccessor(private val bytes: Array[Byte]) extends ByteGraphBytesAccessor {
  override def getByte(offset: Int): Byte = bytes(offset)
  override def getShort(offset: Int): Short = ByteArrayUtils.toShort(bytes, offset)
  override def getInt(offset: Int): Int = ByteArrayUtils.toInt(bytes, offset)
  override def getLong(offset: Int): Long = ByteArrayUtils.toLong(bytes, offset)
  override def getBytes(offset: Int): Array[Byte] = ByteArrayUtils.copy(bytes, offset, bytes.length - offset)
  override def getBytes(offset: Int, length: Int): Array[Byte] = ByteArrayUtils.copy(bytes, offset, length)
  override def slice(offset: Int): ByteGraphBytesSlice = ByteGraphBytesSlice(this, offset, bytes.length - offset)
  override def slice(offset: Int, limit: Int): ByteGraphBytesSlice = ByteGraphBytesSlice(this, offset, limit)
  override def size(): Int = bytes.length
}

object ByteGraphByteArrayAccessor {
  def apply(bytes: Array[Byte]): ByteGraphByteArrayAccessor = new ByteGraphByteArrayAccessor(bytes)
}