package io.exsql.bytegraph.builder

import java.time.{Duration, LocalDate, ZonedDateTime}

import io.exsql.bytegraph.ByteGraph.ByteGraphRow
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphRowType
import io.exsql.bytegraph.{ByteGraph, TemporalValueHelper}
import io.exsql.bytegraph.bytes.{ByteGraphBytes, InMemoryByteGraphBytes}

class ByteGraphRowBuilder(private val offsets: InMemoryByteGraphBytes,
                          private val heap: InMemoryByteGraphBytes,
                          private val schema: ByteGraphRowType) {

  private val startOffset: Int = offsets.position()

  private var index: Int = 0

  final def appendNull(): ByteGraphRowBuilder = {
    offsets.writeInt(-1)
    index += 1
    this
  }

  final def appendBoolean(value: Boolean): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    heap.writeBoolean(value)
    index += 1
    this
  }

  final def appendShort(value: Short): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    heap.writeShort(value)
    index += 1
    this
  }

  final def appendInt(value: Int): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    heap.writeInt(value)
    index += 1
    this
  }

  final def appendLong(value: Long): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    heap.writeLong(value)
    index += 1
    this
  }

  final def appendUShort(value: Short): ByteGraphRowBuilder = appendShort(value)

  final def appendUInt(value: Int): ByteGraphRowBuilder = appendInt(value)

  final def appendULong(value: Long): ByteGraphRowBuilder = appendLong(value)

  final def appendFloat(value: Float): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    heap.writeFloat(value)
    index += 1
    this
  }

  final def appendDouble(value: Double): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    heap.writeDouble(value)
    index += 1
    this
  }

  final def appendDate(value: LocalDate): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    TemporalValueHelper.localDateToBytes(value, heap)
    index += 1
    this
  }

  final def appendDateTime(value: ZonedDateTime): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    TemporalValueHelper.zonedDateTimeToBytes(value, heap)
    index += 1
    this
  }

  final def appendDuration(value: Duration): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    TemporalValueHelper.durationToBytes(value, heap)
    index += 1
    this
  }

  final def appendString(value: String): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    ByteGraphBuilderHelper.writeString(value, heap, writeTypeFlag = false)
    index += 1
    this
  }

  final def appendBlob(value: Array[Byte]): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    ByteGraphBuilderHelper.writeBlob(value, heap, writeTypeFlag = false)
    index += 1
    this
  }

  final def appendList(appendTo: ByteGraphListBuilder => Unit): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    ByteGraphBuilderHelper.writeList(heap, writeTypeFlag = false)(appendTo)
    index += 1
    this
  }

  final def appendStructure(appendTo: ByteGraphStructureBuilder => Unit): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    ByteGraphBuilderHelper.writeStructure(heap, writeTypeFlag = false)(appendTo)
    index += 1
    this
  }

  final def appendRow(appendTo: ByteGraphRowBuilder => Unit): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    ByteGraphBuilderHelper.writeRow(
      heap, schema.fields(index).byteGraphDataType.asInstanceOf[ByteGraphRowType], writeTypeFlag = false
    )(appendTo)

    index += 1
    this
  }

  final def appendReference(field: String): ByteGraphRowBuilder = {
    val refersTo = schema.fieldIndexLookup(field)
    offsets.writeInt(offsets.getInt(startOffset + refersTo * Integer.BYTES))
    index += 1
    this
  }

  final def append(byteGraphBytes: ByteGraphBytes): ByteGraphRowBuilder = {
    offsets.writeInt(heap.position())
    heap.write(byteGraphBytes)

    index += 1
    this
  }

  def build(): ByteGraphRow = {
    val rowByteCount = offsets.length() + heap.length() + java.lang.Integer.BYTES * 2

    offsets.writeInt(startOffset - 2 * java.lang.Integer.BYTES, rowByteCount)
    offsets.writeInt(heap.length())
    offsets.write(heap)
    offsets.close()

    ByteGraphRow(ByteGraph.valueOf(offsets), schema)
  }

}

