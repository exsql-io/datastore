package io.exsql.bytegraph.builder

import java.time.{LocalDate, ZonedDateTime}
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphRowType
import io.exsql.bytegraph.{ByteGraphValueType, TemporalValueHelper, Utf8Utils}
import io.exsql.bytegraph.bytes.{ByteGraphBytes, InMemoryByteGraphBytes}

object ByteGraphBuilderHelper {

  def writeNull(into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Null.typeFlag << 4) + ByteGraphValueType.Null.typeLength)
  }

  def writeBoolean(boolean: Boolean, into: ByteGraphBytes): Unit = {
    if (boolean) into.writeByte((ByteGraphValueType.Boolean.typeFlag << 4) + 1)
    else into.writeByte(ByteGraphValueType.Boolean.typeFlag << 4)
  }

  def writeShort(short: Short, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Short.typeFlag << 4) + ByteGraphValueType.Short.typeLength)
    into.writeShort(short)
  }

  def writeInt(int: Int, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Int.typeFlag << 4) + ByteGraphValueType.Int.typeLength)
    into.writeInt(int)
  }

  def writeLong(long: Long, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Long.typeFlag << 4) + ByteGraphValueType.Long.typeLength)
    into.writeLong(long)
  }

  def writeUShort(short: Short, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.UShort.typeFlag << 4) + ByteGraphValueType.UShort.typeLength)
    into.writeShort(short)
  }

  def writeUInt(int: Int, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.UInt.typeFlag << 4) + ByteGraphValueType.UInt.typeLength)
    into.writeInt(int)
  }

  def writeULong(long: Long, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.ULong.typeFlag << 4) + ByteGraphValueType.ULong.typeLength)
    into.writeLong(long)
  }

  def writeFloat(float: Float, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Float.typeFlag << 4) + ByteGraphValueType.Float.typeLength)
    into.writeInt(java.lang.Float.floatToIntBits(float))
  }

  def writeDouble(double: Double, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Double.typeFlag << 4) + ByteGraphValueType.Double.typeLength)
    into.writeLong(java.lang.Double.doubleToLongBits(double))
  }

  def writeDate(date: LocalDate, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Date.typeFlag << 4) + ByteGraphValueType.Date.typeLength)
    TemporalValueHelper.localDateToBytes(date, into)
  }

  def writeDateTime(dateTime: ZonedDateTime, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.DateTime.typeFlag << 4) + ByteGraphValueType.DateTime.typeLength)
    TemporalValueHelper.zonedDateTimeToBytes(dateTime, into)
  }

  def writeDuration(duration: java.time.Duration, into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.Duration.typeFlag << 4) + ByteGraphValueType.Duration.typeLength)
    TemporalValueHelper.durationToBytes(duration, into)
  }

  def writeString(string: String, into: ByteGraphBytes, writeTypeFlag: Boolean = true): Unit = {
    if (writeTypeFlag) into.writeByte((ByteGraphValueType.String.typeFlag << 4) + ByteGraphValueType.String.typeLength)

    val lengthOffset = into.position()
    into.writeInt(0)

    val length = Utf8Utils.encode(string, into)
    into.writeInt(lengthOffset, length)
  }

  def writeUtf8Bytes(bytes: Array[Byte], into: ByteGraphBytes): Unit = {
    into.writeByte((ByteGraphValueType.String.typeFlag << 4) + ByteGraphValueType.String.typeLength)
    writeBytes(bytes, into)
  }

  def writeBlob(bytes: Array[Byte], into: ByteGraphBytes, writeTypeFlag: Boolean = true): Unit = {
    if (writeTypeFlag) into.writeByte((ByteGraphValueType.Blob.typeFlag << 4) + ByteGraphValueType.Blob.typeLength)
    writeBytes(bytes, into)
  }

  def writeList(into: InMemoryByteGraphBytes, writeTypeFlag: Boolean = true)
               (appendTo: ByteGraphListBuilder => Unit): Unit = {

    if (writeTypeFlag) into.writeByte((ByteGraphValueType.List.typeFlag << 4) + ByteGraphValueType.List.typeLength)

    val lengthOffset = into.position()
    into.writeInt(0)

    val builder = new ByteGraphListBuilder(into)
    appendTo(builder)

    val length = into.position() - (lengthOffset + java.lang.Integer.BYTES)
    into.writeInt(lengthOffset, length)
  }

  def writeStructure(into: InMemoryByteGraphBytes, writeTypeFlag: Boolean = true)
                    (appendTo: ByteGraphStructureBuilder => Unit): Unit = {

    if (writeTypeFlag)
      into.writeByte((ByteGraphValueType.Structure.typeFlag << 4) + ByteGraphValueType.Structure.typeLength)

    val lengthOffset = into.position()
    into.writeInt(0)

    val builder = new ByteGraphStructureBuilder(into)
    appendTo(builder)

    val length = into.position() - (lengthOffset + java.lang.Integer.BYTES)
    into.writeInt(lengthOffset, length)
  }

  def writeRow(into: InMemoryByteGraphBytes, schema: ByteGraphRowType, writeTypeFlag: Boolean = true)
              (appendTo: ByteGraphRowBuilder => Unit): Unit = {

    val offsets = InMemoryByteGraphBytes()
    val heap = InMemoryByteGraphBytes()

    val builder = new ByteGraphRowBuilder(offsets, heap, schema)
    appendTo(builder)

    val length: Int = offsets.length() + heap.length() + java.lang.Integer.BYTES * 2

    if (writeTypeFlag) into.writeByte((ByteGraphValueType.Row.typeFlag << 4) + ByteGraphValueType.Row.typeLength)
    into.writeInt(length)
    into.writeInt(offsets.length())
    into.write(offsets)
    into.writeInt(heap.length())
    into.write(heap)
  }

  private[builder] def writeBytes(bytes: Array[Byte], into: ByteGraphBytes): Unit = {
    into.writeInt(bytes.length)
    into.write(bytes)
  }

}
