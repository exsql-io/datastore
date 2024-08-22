package io.exsql.bytegraph.builder

import java.time.{Duration, LocalDate, ZonedDateTime}
import io.exsql.bytegraph.ByteGraph.ByteGraphStructure
import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.bytes.{ByteGraphBytes, InMemoryByteGraphBytes}
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphRowType

class ByteGraphStructureBuilder private[bytegraph](private val into: InMemoryByteGraphBytes) {

  private val startOffset: Int = into.position()

  final def appendNull(name: String): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeNull(into)
    this
  }

  final def appendBoolean(name: String, value: Boolean): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeBoolean(value, into)
    this
  }

  final def appendShort(name: String, value: Short): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeShort(value, into)
    this
  }

  final def appendInt(name: String, value: Int): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeInt(value, into)
    this
  }

  final def appendLong(name: String, value: Long): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeLong(value, into)
    this
  }

  final def appendUShort(name: String, value: Short): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeUShort(value, into)
    this
  }

  final def appendUInt(name: String, value: Int): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeUInt(value, into)
    this
  }

  final def appendULong(name: String, value: Long): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeULong(value, into)
    this
  }

  final def appendFloat(name: String, value: Float): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeFloat(value, into)
    this
  }

  final def appendDouble(name: String, value: Double): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeDouble(value, into)
    this
  }

  final def appendDate(name: String, value: LocalDate): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeDate(value, into)
    this
  }

  final def appendDateTime(name: String, value: ZonedDateTime): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeDateTime(value, into)
    this
  }

  final def appendDuration(name: String, value: Duration): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeDuration(value, into)
    this
  }

  final def appendString(name: String, value: String): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeString(value, into)
    this
  }

  final def appendUtf8Bytes(name: String, value: Array[Byte]): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    into.write(value)
    this
  }

  final def appendBlob(name: String, value: Array[Byte]): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeBlob(value, into)
    this
  }

  final def appendList(name: String)(appendTo: ByteGraphListBuilder => Unit): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeList(into)(appendTo)
    this
  }

  final def appendStructure(name: String)(appendTo: ByteGraphStructureBuilder => Unit): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeStructure(into)(appendTo)
    this
  }

  final def appendRow(name: String, schema: ByteGraphRowType)
                     (appendTo: ByteGraphRowBuilder => Unit): ByteGraphStructureBuilder = {

    ByteGraphBuilderHelper.writeString(name, into)
    ByteGraphBuilderHelper.writeRow(into, schema)(appendTo)
    this
  }

  final def append(name: String, byteGraphBytes: ByteGraphBytes): ByteGraphStructureBuilder = {
    ByteGraphBuilderHelper.writeString(name, into)
    into.write(byteGraphBytes)
    this
  }

  def build(): ByteGraphStructure = {
    val length = into.position() - startOffset
    into.writeInt(startOffset - java.lang.Integer.BYTES, length)
    into.close()

    ByteGraphStructure(ByteGraph.valueOf(into))
  }

}
