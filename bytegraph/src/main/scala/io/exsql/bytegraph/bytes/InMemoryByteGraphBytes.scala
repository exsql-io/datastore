package io.exsql.bytegraph.bytes

import io.exsql.bytegraph.{ByteArrayUtils, FasterByteArrayOutputStream}

class InMemoryByteGraphBytes(private val byteArrayOutputStream: FasterByteArrayOutputStream) extends ByteGraphBytes {

  @inline final override def writeInt(int: Int): Unit = {
    byteArrayOutputStream.writeInt(int)
  }

  @inline final override def writeInt(offset: Int, int: Int): Unit = {
    byteArrayOutputStream.writeInt(offset, int)
  }

  @inline final override def writeByte(byte: Int): Unit = byteArrayOutputStream.write(byte)

  @inline final override def writeShort(short: Short): Unit = byteArrayOutputStream.writeShort(short)

  @inline final override def writeLong(long: Long): Unit = {
    byteArrayOutputStream.writeLong(long)
  }

  @inline final override def write(bytes: Array[Byte]): Unit = byteArrayOutputStream.write(bytes)

  @inline final def write(byteGraphBytes: ByteGraphBytes): Unit = {
    byteGraphBytes match {
      case inMemoryByteGraphBytes: InMemoryByteGraphBytes => byteArrayOutputStream.write(inMemoryByteGraphBytes.byteArrayOutputStream.array, 0, inMemoryByteGraphBytes.byteArrayOutputStream.length)
      case _ => byteArrayOutputStream.write(byteGraphBytes.bytes())
    }
  }

  @inline final def write(byteGraphBytes: ByteGraphBytes, offset: Int): Unit = {
    byteGraphBytes match {
      case inMemoryByteGraphBytes: InMemoryByteGraphBytes => byteArrayOutputStream.write(inMemoryByteGraphBytes.byteArrayOutputStream.array, offset, inMemoryByteGraphBytes.byteArrayOutputStream.length)
      case _ => byteArrayOutputStream.write(byteGraphBytes.getBytes(offset))
    }
  }

  @inline final override def write(bytes: Array[Byte], start: Int, length: Int): Unit = byteArrayOutputStream.write(bytes, start, length)

  @inline final override def position(): Int = byteArrayOutputStream.position().toInt

  @inline final override def position(offset: Int): Unit = byteArrayOutputStream.position(offset.toLong)

  @inline final override def length(): Int = byteArrayOutputStream.length

  @inline final override def bytes(): Array[Byte] = {
    byteArrayOutputStream.bytes()
  }

  @inline final override def close(): Unit = {
    byteArrayOutputStream.close()
  }

  override def getByte(offset: Int): Byte = {
    byteArrayOutputStream.array(offset)
  }

  override def getShort(offset: Int): Short = {
    ByteArrayUtils.toShort(byteArrayOutputStream.array, offset)
  }

  override def getInt(offset: Int): Int = {
    ByteArrayUtils.toInt(byteArrayOutputStream.array, offset)
  }

  override def getLong(offset: Int): Long = {
    ByteArrayUtils.toLong(byteArrayOutputStream.array, offset)
  }

  override def getBytes(offset: Int): Array[Byte] = {
    ByteArrayUtils.copy(byteArrayOutputStream.array, offset, byteArrayOutputStream.length - offset)
  }

  override def getBytes(offset: Int, length: Int): Array[Byte] = {
    ByteArrayUtils.copy(byteArrayOutputStream.array, offset, length)
  }

  override def slice(offset: Int): ByteGraphBytesSlice = {
    ByteGraphBytesSlice(this, offset, byteArrayOutputStream.length - offset)
  }

  override def slice(offset: Int, limit: Int): ByteGraphBytesSlice = {
    ByteGraphBytesSlice(this, offset, limit)
  }

  override def size(): Int = {
    byteArrayOutputStream.length
  }

}

object InMemoryByteGraphBytes {

  def apply(): InMemoryByteGraphBytes = {
    new InMemoryByteGraphBytes(new FasterByteArrayOutputStream())
  }

  def apply(initialCapacity: Int): InMemoryByteGraphBytes = {
    new InMemoryByteGraphBytes(new FasterByteArrayOutputStream(initialCapacity))
  }

}