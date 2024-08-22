package io.exsql.bytegraph.builder

import java.time.{Duration, LocalDate, ZonedDateTime}
import io.exsql.bytegraph.ByteGraph.ByteGraphList
import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.bytes.{ByteGraphBytes, InMemoryByteGraphBytes}
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphRowType

class ByteGraphListBuilder private[bytegraph] (private val into: InMemoryByteGraphBytes) {

  private val startOffset: Int = into.position()

  final def appendNull(): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeNull(into)
    this
  }

  final def appendBoolean(value: Boolean): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeBoolean(value, into)
    this
  }

  final def appendShort(value: Short): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeShort(value, into)
    this
  }

  final def appendInt(value: Int): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeInt(value, into)
    this
  }

  final def appendLong(value: Long): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeLong(value, into)
    this
  }

  final def appendUShort(value: Short): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeUShort(value, into)
    this
  }

  final def appendUInt(value: Int): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeUInt(value, into)
    this
  }

  final def appendULong(value: Long): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeULong(value, into)
    this
  }

  final def appendFloat(value: Float): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeFloat(value, into)
    this
  }

  final def appendDouble(value: Double): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeDouble(value, into)
    this
  }

  final def appendDate(value: LocalDate): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeDate(value, into)
    this
  }

  final def appendDateTime(value: ZonedDateTime): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeDateTime(value, into)
    this
  }

  final def appendDuration(value: Duration): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeDuration(value, into)
    this
  }

  final def appendString(value: String): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeString(value, into)
    this
  }

  final def appendUtf8Bytes(value: Array[Byte]): ByteGraphListBuilder = {
    into.writeInt(value.length)
    into.write(value)
    this
  }

  final def appendBlob(value: Array[Byte]): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeBlob(value, into)
    this
  }

  final def appendList(appendTo: ByteGraphListBuilder => Unit): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeList(into)(appendTo)
    this
  }

  final def appendStructure(appendTo: ByteGraphStructureBuilder => Unit): ByteGraphListBuilder = {
    ByteGraphBuilderHelper.writeStructure(into)(appendTo)
    this
  }

  final def appendRow(schema: ByteGraphRowType)
                     (appendTo: ByteGraphRowBuilder => Unit): ByteGraphListBuilder = {

    ByteGraphBuilderHelper.writeRow(into, schema)(appendTo)
    this
  }

  final def append(byteGraphBytes: ByteGraphBytes): ByteGraphListBuilder = {
    into.write(byteGraphBytes)
    this
  }

  def build(): ByteGraphList = {
    val length = into.position() - startOffset
    into.writeInt(startOffset - java.lang.Integer.BYTES, length)
    into.close()

    ByteGraphList(ByteGraph.valueOf(into))
  }

}
