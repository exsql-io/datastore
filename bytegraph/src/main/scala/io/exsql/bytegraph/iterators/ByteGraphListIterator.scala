package io.exsql.bytegraph.iterators

import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphList}
import io.exsql.bytegraph.lookups.ByteGraphListLookup
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphDataType

private[bytegraph] class ByteGraphListIterator(private val container: ByteGraphList,
                                               private val elementType: Option[ByteGraphDataType]) extends Iterator[ByteGraph] {

  private val byteGraphBytes = container.bytes.slice(5)

  override def hasNext: Boolean = {
    byteGraphBytes.remainingBytes() > 0
  }

  override def next(): ByteGraph = {
    ByteGraphListLookup.read(byteGraphBytes, elementType)
  }

}
