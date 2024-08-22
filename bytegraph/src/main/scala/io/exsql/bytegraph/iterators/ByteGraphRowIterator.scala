package io.exsql.bytegraph.iterators

import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphRow}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphReferenceType, ByteGraphSchema}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import io.exsql.bytegraph.builder.ByteGraphBuilderHelper
import io.exsql.bytegraph.bytes.InMemoryByteGraphBytes
import io.exsql.bytegraph.lookups.ByteGraphRowLookup

private[bytegraph] class ByteGraphRowIterator(private val container: ByteGraphRow,
                                              private val schema: ByteGraphSchema) extends Iterator[ByteGraph] {

  private val byteGraphBytes = container.bytes.slice(5)

  private val offsets = {
    val length = byteGraphBytes.readInt()
    byteGraphBytes.readView(length)
  }

  private val heap = {
    val length = byteGraphBytes.readInt()
    byteGraphBytes.readView(length)
  }

  private var index = 0

  override def hasNext: Boolean = {
    index < schema.fields.length
  }

  override def next(): ByteGraph = {
    val field = schema.fields(index)
    val offset = offsets.getInt(index * Integer.BYTES)
    index += 1

    val bytes = InMemoryByteGraphBytes()
    if (offset == -1) {
      ByteGraphBuilderHelper.writeNull(bytes)
      return ByteGraph.valueOf(bytes)
    }

    field.byteGraphDataType.valueType match {
      case ByteGraphValueType.Reference =>
        val referenced = schema.fieldLookup(field.byteGraphDataType.asInstanceOf[ByteGraphReferenceType].field)
        ByteGraphRowLookup.read(referenced, heap, offset, bytes)

      case _ => ByteGraphRowLookup.read(field, heap, offset, bytes)
    }

  }

}
