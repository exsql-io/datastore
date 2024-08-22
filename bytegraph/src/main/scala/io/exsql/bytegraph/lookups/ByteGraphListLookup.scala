package io.exsql.bytegraph.lookups

import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphDataType, ByteGraphRowType}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import io.exsql.bytegraph.builder.ByteGraphBuilderHelper
import io.exsql.bytegraph.bytes.{ByteGraphBytesSlice, InMemoryByteGraphBytes}

class ByteGraphListLookup(private val container: ByteGraphList) {

  def apply(index: Int): Option[ByteGraph] = {
    val byteBuf = container.bytes.slice(5)
    var position = 0
    while (byteBuf.remainingBytes() > 0 && position < index) {
      skip(byteBuf)
      position += 1
    }

    if (byteBuf.remainingBytes() > 0) Some(ByteGraphListLookup.read(byteBuf, container.elementType))
    else None
  }

  private def skip(byteGraphBytesView: ByteGraphBytesSlice): Unit = {
    val byte = byteGraphBytesView.readByte()
    ByteGraphValueType.readValueTypeFromByte(byte) match {
      case ByteGraphValueType.String | ByteGraphValueType.Blob | ByteGraphValueType.List | ByteGraphValueType.Structure | ByteGraphValueType.Row =>
        val length = byteGraphBytesView.readInt()
        byteGraphBytesView.skipBytes(length)

      case ByteGraphValueType.Null | ByteGraphValueType.Boolean => ()
      case byteGraphValueType => byteGraphBytesView.skipBytes(byteGraphValueType.typeLength)
    }
  }

}

private[bytegraph] object ByteGraphListLookup {

  def read(byteGraphBytes: ByteGraphBytesSlice, elementType: Option[ByteGraphDataType]): ByteGraph = {
    val byte = byteGraphBytes.readByte()

    ByteGraphValueType.readValueTypeFromByte(byte) match {
      case ByteGraphValueType.String | ByteGraphValueType.Blob =>
        val length = byteGraphBytes.readInt()
        val slice = byteGraphBytes.slice(byteGraphBytes.position() - 5, length + 5)
        byteGraphBytes.skipBytes(length)

        ByteGraph.valueOf(slice)

      case ByteGraphValueType.List =>
        val length = byteGraphBytes.readInt()
        val byteGraphList = ByteGraphList(ByteGraph.valueOf(byteGraphBytes.slice(byteGraphBytes.position() - 5, length + 5)))
        byteGraphBytes.skipBytes(length)

        byteGraphList

      case ByteGraphValueType.Structure =>
        val length = byteGraphBytes.readInt()
        val byteGraphStructure = ByteGraphStructure(ByteGraph.valueOf(byteGraphBytes.slice(byteGraphBytes.position() - 5, length + 5)))
        byteGraphBytes.skipBytes(length)

        byteGraphStructure

      case ByteGraphValueType.Row =>
        val length = byteGraphBytes.readInt()
        val byteGraphRow = ByteGraphRow(
          ByteGraph.valueOf(byteGraphBytes.slice(byteGraphBytes.position() - 5, length + 5)),
          elementType.get.asInstanceOf[ByteGraphRowType]
        )

        byteGraphBytes.skipBytes(length)

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
            val slice = byteGraphBytes.slice(byteGraphBytes.position() - 1, byteGraphValueType.typeLength + 1)
            byteGraphBytes.skipBytes(byteGraphValueType.typeLength)

            ByteGraph.valueOf(slice)
        }
    }
  }

}