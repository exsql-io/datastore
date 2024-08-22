package io.exsql.bytegraph.bytes

class ByteGraphBytesSlice(private val reader: ByteGraphBytesAccessor,
                          private val offset: Int,
                          private val limit: Int) extends ByteGraphBytes {

  private var readPosition: Int = 0

  override def getByte(offset: Int): Byte = reader.getByte(this.offset + offset)
  override def getShort(offset: Int): Short = reader.getShort(this.offset + offset)
  override def getInt(offset: Int): Int = reader.getInt(this.offset + offset)
  override def getLong(offset: Int): Long = reader.getLong(this.offset + offset)
  override def getBytes(offset: Int): Array[Byte] = {
    val position = this.offset + offset
    reader.getBytes(position, this.limit - offset)
  }

  override def getBytes(offset: Int, length: Int): Array[Byte] = reader.getBytes(this.offset + offset, length)
  override def slice(offset: Int): ByteGraphBytesSlice = ByteGraphBytesSlice(this.reader, this.offset + offset, this.limit - offset)
  override def slice(offset: Int, limit: Int): ByteGraphBytesSlice = ByteGraphBytesSlice(this.reader, this.offset + offset, limit)
  override def size(): Int = length()

  def skipBytes(count: Int): Unit = readPosition += count

  def readByte(): Byte = {
    val value = getByte(readPosition)
    readPosition += java.lang.Byte.BYTES

    value
  }

  def readShort(): Short = {
    val value = getShort(readPosition)
    readPosition += java.lang.Short.BYTES

    value
  }

  def readInt(): Int = {
    val value = getInt(readPosition)
    readPosition += java.lang.Integer.BYTES

    value
  }

  def readLong(): Long = {
    val value = getLong(readPosition)
    readPosition += java.lang.Long.BYTES

    value
  }

  def readBytes(): Array[Byte] = {
    val value = getBytes(readPosition, limit - readPosition)
    readPosition = limit

    value
  }

  def readBytes(length: Int): Array[Byte] = {
    val value = getBytes(readPosition, length)
    readPosition += length

    value
  }

  def readView(length: Int): ByteGraphBytesSlice = {
    val value = slice(readPosition, length)
    readPosition += length

    value
  }

  def compareBytesTo(offset: Int, length: Int, bytes: Array[Byte]): Int = {
    if (length != bytes.length) length.compareTo(bytes.length)
    else {
      val size = length
      var index = 0
      while (index < size) {
        val compareTo = getByte(offset + index).compareTo(bytes(index))
        if (compareTo != 0) return compareTo

        index += 1
      }

      0
    }
  }

  def remainingBytes(): Int = limit - readPosition

  override def position(): Int = readPosition
  override def position(offset: Int): Unit = readPosition = offset

  override def length(): Int = limit

  override def bytes(): Array[Byte] = getBytes(0)

  override def writeByte(byte: Int): Unit = throw new UnsupportedOperationException("ByteGraphBytesView is read-only")
  override def writeShort(short: Short): Unit = throw new UnsupportedOperationException("ByteGraphBytesView is read-only")
  override def writeInt(int: Int): Unit = throw new UnsupportedOperationException("ByteGraphBytesView is read-only")
  override def writeInt(offset: Int, int: Int): Unit = throw new UnsupportedOperationException("ByteGraphBytesView is read-only")
  override def writeLong(long: Long): Unit = throw new UnsupportedOperationException("ByteGraphBytesView is read-only")
  override def write(bytes: Array[Byte]): Unit = throw new UnsupportedOperationException("ByteGraphBytesView is read-only")
  override def write(bytes: Array[Byte], start: Int, length: Int): Unit = throw new UnsupportedOperationException("ByteGraphBytesView is read-only")
  override def close(): Unit = ()

}

object ByteGraphBytesSlice {

  def apply(reader: ByteGraphBytesAccessor): ByteGraphBytesSlice = {
    new ByteGraphBytesSlice(reader, 0, reader.size())
  }

  def apply(reader: ByteGraphBytesAccessor, offset: Int, length: Int): ByteGraphBytesSlice = {
    new ByteGraphBytesSlice(reader, offset, length)
  }

}