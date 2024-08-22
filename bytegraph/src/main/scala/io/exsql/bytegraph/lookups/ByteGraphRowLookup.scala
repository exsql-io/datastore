package io.exsql.bytegraph.lookups

import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphBasicDataType, ByteGraphListType, ByteGraphReferenceType, ByteGraphRowType, ByteGraphSchemaField, ByteGraphStructureType}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import io.exsql.bytegraph.builder.ByteGraphBuilderHelper
import io.exsql.bytegraph.bytes.{ByteGraphBytes, ByteGraphBytesSlice, InMemoryByteGraphBytes}

private[bytegraph] class ByteGraphRowLookup(private val container: ByteGraphRow) {

  def apply(index: Int): Option[ByteGraph] = {
    val byteBuf = container.bytes.slice(5)
    val offsets = {
      val length = byteBuf.readInt()
      byteBuf.readView(length)
    }

    val heap = {
      val length = byteBuf.readInt()
      byteBuf.readView(length)
    }

    val field = container.rowSchema.fields(index)
    val offset = offsets.getInt(index * Integer.BYTES)

    val bytes = InMemoryByteGraphBytes()
    if (offset == -1) {
      ByteGraphBuilderHelper.writeNull(bytes)
      return Some(ByteGraph.valueOf(bytes))
    }

    field.byteGraphDataType.valueType match {
      case ByteGraphValueType.Reference =>
        val referenced = container.rowSchema.fieldLookup(field.byteGraphDataType.asInstanceOf[ByteGraphReferenceType].field)
        Some(ByteGraphRowLookup.read(referenced, heap, offset, bytes))

      case _ => Some(ByteGraphRowLookup.read(field, heap, offset, bytes))
    }
  }

}

private[bytegraph] object ByteGraphRowLookup {

  def read(field: ByteGraphSchemaField,
           heap: ByteGraphBytesSlice,
           offset: Int,
           byteGraphBytes: ByteGraphBytes): ByteGraph = {

    field.byteGraphDataType.valueType match {
      case ByteGraphValueType.Null =>
        ByteGraphBuilderHelper.writeNull(byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Boolean =>
        ByteGraphBuilderHelper.writeBoolean(heap.getByte(offset) != 0, byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Short | ByteGraphValueType.UShort =>
        ByteGraphBuilderHelper.writeShort(heap.getShort(offset), byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Int | ByteGraphValueType.UInt =>
        ByteGraphBuilderHelper.writeInt(heap.getInt(offset), byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Long | ByteGraphValueType.ULong =>
        ByteGraphBuilderHelper.writeLong(heap.getLong(offset), byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Float =>
        ByteGraphBuilderHelper.writeFloat(heap.getFloat(offset), byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Double =>
        ByteGraphBuilderHelper.writeDouble(heap.getDouble(offset), byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Date =>
        byteGraphBytes.writeByte((ByteGraphValueType.Date.typeFlag << 4) + ByteGraphValueType.Date.typeLength)
        byteGraphBytes.write(heap.getBytes(offset, ByteGraphValueType.Date.typeLength))
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.DateTime =>
        byteGraphBytes.writeByte((ByteGraphValueType.DateTime.typeFlag << 4) + ByteGraphValueType.DateTime.typeLength)
        byteGraphBytes.write(heap.getBytes(offset, ByteGraphValueType.DateTime.typeLength))
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Duration =>
        byteGraphBytes.writeByte((ByteGraphValueType.Duration.typeFlag << 4) + ByteGraphValueType.Duration.typeLength)
        byteGraphBytes.write(heap.getBytes(offset, ByteGraphValueType.Duration.typeLength))
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.String | ByteGraphValueType.Symbol =>
        byteGraphBytes.writeByte((ByteGraphValueType.String.typeFlag << 4) + ByteGraphValueType.String.typeLength)
        val length = heap.getInt(offset)
        byteGraphBytes.write(heap.getBytes(offset, length + Integer.BYTES))
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.Blob =>
        byteGraphBytes.writeByte((ByteGraphValueType.Blob.typeFlag << 4) + ByteGraphValueType.Blob.typeLength)
        val length = heap.getInt(offset)
        byteGraphBytes.write(heap.getBytes(offset, length + Integer.BYTES))
        ByteGraph.valueOf(byteGraphBytes)

      case ByteGraphValueType.List =>
        byteGraphBytes.writeByte((ByteGraphValueType.List.typeFlag << 4) + ByteGraphValueType.List.typeLength)
        val length = heap.getInt(offset)
        byteGraphBytes.write(heap.getBytes(offset, length + Integer.BYTES))

        if (field.byteGraphDataType.isInstanceOf[ByteGraphBasicDataType]) ByteGraphList(ByteGraph.valueOf(byteGraphBytes))
        else ByteGraphList(ByteGraph.valueOf(byteGraphBytes), Some(field.byteGraphDataType.asInstanceOf[ByteGraphListType].elements))

      case ByteGraphValueType.Structure =>
        byteGraphBytes.writeByte((ByteGraphValueType.Structure.typeFlag << 4) + ByteGraphValueType.Structure.typeLength)
        val length = heap.getInt(offset)
        byteGraphBytes.write(heap.getBytes(offset, length + Integer.BYTES))

        if (field.byteGraphDataType.isInstanceOf[ByteGraphBasicDataType]) ByteGraphStructure(ByteGraph.valueOf(byteGraphBytes))
        else ByteGraphStructure(ByteGraph.valueOf(byteGraphBytes), Some(field.byteGraphDataType.asInstanceOf[ByteGraphStructureType]))

      case ByteGraphValueType.Row =>
        byteGraphBytes.writeByte((ByteGraphValueType.Row.typeFlag << 4) + ByteGraphValueType.Row.typeLength)
        val length = heap.getInt(offset)
        byteGraphBytes.write(heap.getBytes(offset, length + Integer.BYTES))
        ByteGraphRow(ByteGraph.valueOf(byteGraphBytes), field.byteGraphDataType.asInstanceOf[ByteGraphRowType])
    }
  }

}
