package io.exsql.bytegraph.lookups

import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphRowType, ByteGraphSchema}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType, Utf8Utils}
import io.exsql.bytegraph.builder.ByteGraphBuilderHelper
import io.exsql.bytegraph.bytes.{ByteGraphBytesSlice, InMemoryByteGraphBytes}

private[bytegraph] class ByteGraphStructureLookup(private val container: ByteGraphStructure) {

  def apply(index: Int): Option[ByteGraph] = {
    val slice = container.bytes.slice(5)
    var position = 0
    while (slice.remainingBytes() > 0 && position < index) {
      skipEntry(slice)
      position += 1
    }

    if (slice.remainingBytes() > 0) Some(readEntryValue(slice))
    else None
  }

  def apply(name: String): Option[ByteGraph] = {
    val slice = container.bytes.slice(5)
    val nameBytes = Utf8Utils.encode(name)

    while (slice.remainingBytes() > 0) {
      slice.skipBytes(1)
      val fieldLength = slice.readInt()
      val hasSameFieldName = slice.compareBytesTo(slice.position(), fieldLength, nameBytes) == 0
      slice.skipBytes(fieldLength)

      if (hasSameFieldName) return Some(ByteGraphStructureLookup.readByteGraphValue(slice))
      else skipEntryValue(slice)
    }

    None
  }

  private def skipEntry(slice: ByteGraphBytesSlice): Unit = {
    // skipping field type all strings for now
    slice.skipBytes(1)
    val fieldLength = slice.readInt()
    slice.skipBytes(fieldLength)
    skipEntryValue(slice)
  }

  private def skipEntryValue(slice: ByteGraphBytesSlice): Unit = {
    val byte = slice.readByte()
    ByteGraphValueType.readValueTypeFromByte(byte) match {
      case ByteGraphValueType.String | ByteGraphValueType.Blob | ByteGraphValueType.List | ByteGraphValueType.Structure | ByteGraphValueType.Row =>
        val length = slice.readInt()
        slice.skipBytes(length)

      case ByteGraphValueType.Null | ByteGraphValueType.Boolean => ()
      case byteGraphValueType => slice.skipBytes(byteGraphValueType.typeLength)
    }
  }

  private def readEntryValue(slice: ByteGraphBytesSlice): ByteGraph = {
    // skipping field type all strings for now
    slice.skipBytes(1)
    val fieldLength = slice.readInt()
    slice.skipBytes(fieldLength)

    ByteGraphStructureLookup.readByteGraphValue(slice)
  }

}

private[bytegraph] object ByteGraphStructureLookup {

  def readByteGraphValue(slice: ByteGraphBytesSlice,
                         field: Option[String] = None,
                         schema: Option[ByteGraphSchema] = None): ByteGraph = {

    val byte = slice.readByte()
    ByteGraphValueType.readValueTypeFromByte(byte) match {
      case ByteGraphValueType.String | ByteGraphValueType.Blob =>
        val length = slice.readInt()
        val sliced = slice.slice(slice.position() - 5)
        slice.skipBytes(length)

        ByteGraph.valueOf(sliced)

      case ByteGraphValueType.List =>
        val length = slice.readInt()
        val byteGraphList = ByteGraphList(ByteGraph.valueOf(slice.slice(slice.position() - 5, length + 5)))
        slice.skipBytes(length)

        byteGraphList

      case ByteGraphValueType.Structure =>
        val length = slice.readInt()
        val byteGraphStructure = ByteGraphStructure(ByteGraph.valueOf(slice.slice(slice.position() - 5, length + 5)))
        slice.skipBytes(length)

        byteGraphStructure

      case ByteGraphValueType.Row =>
        val length = slice.readInt()
        val byteGraphRow = ByteGraphRow(
          ByteGraph.valueOf(slice.slice(slice.position() - 5, length + 5)),
          schema.get.fieldLookup(field.get).byteGraphDataType.asInstanceOf[ByteGraphRowType]
        )

        slice.skipBytes(length)

        byteGraphRow

      case byteGraphValueType =>
        byteGraphValueType match {
          case ByteGraphValueType.Null =>
            val bytes = InMemoryByteGraphBytes(1)
            ByteGraphBuilderHelper.writeNull(bytes)
            ByteGraph.valueOf(bytes)

          case ByteGraphValueType.Boolean =>
            val bytes = InMemoryByteGraphBytes(1)
            ByteGraphBuilderHelper.writeBoolean(ByteGraphValueType.readTypeLengthFromByte(byte) == 1, bytes)
            ByteGraph.valueOf(bytes)

          case _ =>
            val sliced = slice.slice(slice.position() - 1)
            slice.skipBytes(byteGraphValueType.typeLength)

            ByteGraph.valueOf(sliced)
        }
    }
  }

}