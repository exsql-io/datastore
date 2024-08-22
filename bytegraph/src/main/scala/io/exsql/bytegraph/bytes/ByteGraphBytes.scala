package io.exsql.bytegraph.bytes

trait ByteGraphBytes extends ByteGraphBytesAccessor {

  def writeByte(byte: Int): Unit
  def writeShort(short: Short): Unit
  def writeInt(int: Int): Unit
  def writeInt(offset: Int, int: Int): Unit
  def writeLong(long: Long): Unit
  def write(bytes: Array[Byte]): Unit
  def write(bytes: Array[Byte], start: Int, length: Int): Unit

  @inline final def writeBoolean(boolean: Boolean): Unit = {
    if (boolean) writeByte(1) else writeByte(0)
  }

  @inline final def writeFloat(float: Float): Unit = writeInt(java.lang.Float.floatToIntBits(float))

  @inline final def writeDouble(double: Double): Unit = writeLong(java.lang.Double.doubleToLongBits(double))

  def position(): Int
  def position(int: Int): Unit

  def length(): Int

  def bytes(): Array[Byte]
  def close(): Unit

}
