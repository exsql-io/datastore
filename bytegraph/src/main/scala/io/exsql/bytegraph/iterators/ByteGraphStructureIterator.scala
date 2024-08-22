package io.exsql.bytegraph.iterators

import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphStructure}
import io.exsql.bytegraph.Utf8Utils
import io.exsql.bytegraph.lookups.ByteGraphStructureLookup
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchema

private[bytegraph] class ByteGraphStructureIterator(private val container: ByteGraphStructure,
                                                    private val schema: Option[ByteGraphSchema]) extends Iterator[(String, ByteGraph)] {

  private val byteGraphBytes = container.bytes.slice(5)

  override def hasNext: Boolean = {
    byteGraphBytes.remainingBytes() > 0
  }

  override def next(): (String, ByteGraph) = {
    // skipping field type all strings for now
    byteGraphBytes.skipBytes(1)

    val fieldLength = byteGraphBytes.readInt()
    val field = Utf8Utils.decode(byteGraphBytes.readBytes(fieldLength))

    field -> ByteGraphStructureLookup.readByteGraphValue(byteGraphBytes, Some(field), schema)
  }

}
