package io.exsql.bytegraph.bytes

class ByteArrayByteGraphBytes(private val accessor: ByteGraphBytesAccessor) extends ByteGraphBytes {

  override def getByte(offset: Int): Byte = accessor.getByte(offset)

  override def getShort(offset: Int): Short = accessor.getShort(offset)

  override def getInt(offset: Int): Int = accessor.getInt(offset)

  override def getLong(offset: Int): Long = accessor.getLong(offset)

  override def getBytes(offset: Int): Array[Byte] = accessor.getBytes(offset)

  override def getBytes(offset: Int, length: Int): Array[Byte] = accessor.getBytes(offset, length)

  override def slice(offset: Int): ByteGraphBytesSlice = accessor.slice(offset)

  override def slice(offset: Int, limit: Int): ByteGraphBytesSlice = accessor.slice(offset, limit)

  override def bytes(): Array[Byte] = getBytes(0)

  override def length(): Int = accessor.size()

  override def size(): Int = accessor.size()

  override def position(): Int = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes has no relative position")
  override def position(offset: Int): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes has no relative position")
  override def writeByte(byte: Int): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes is read-only")
  override def writeShort(short: Short): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes is read-only")
  override def writeInt(int: Int): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes is read-only")
  override def writeInt(offset: Int, int: Int): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes is read-only")
  override def writeLong(long: Long): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes is read-only")
  override def write(bytes: Array[Byte]): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes is read-only")
  override def write(bytes: Array[Byte], start: Int, length: Int): Unit = throw new UnsupportedOperationException("ReadOnlyByteGraphBytes is read-only")
  override def close(): Unit = ()
}

object ByteArrayByteGraphBytes {

  def apply(bytes: Array[Byte]): ByteGraphBytes = {
    new ByteArrayByteGraphBytes(ByteGraphByteArrayAccessor(bytes))
  }

  def apply(bytes: Array[Byte], offset: Int, length: Int): ByteGraphBytes = {
    new ByteArrayByteGraphBytes(ByteGraphByteArrayAccessor(bytes)).slice(offset, length)
  }

}