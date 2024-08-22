package io.exsql.bytegraph.bytes

trait ByteGraphBytesAccessor {

  def getByte(offset: Int): Byte
  def getShort(offset: Int): Short
  def getInt(offset: Int): Int
  def getLong(offset: Int): Long
  def getBytes(offset: Int): Array[Byte]
  def getBytes(offset: Int, length: Int): Array[Byte]

  @inline final def getBoolean(offset: Int): Boolean = getByte(offset) == 1
  @inline final def getFloat(offset: Int): Float = java.lang.Float.intBitsToFloat(getInt(offset))
  @inline final def getDouble(offset: Int): Double = java.lang.Double.longBitsToDouble(getLong(offset))

  def slice(offset: Int): ByteGraphBytesSlice
  def slice(offset: Int, limit: Int): ByteGraphBytesSlice

  def size(): Int

}
