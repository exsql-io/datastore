package io.exsql.datastore.controller.kafka

import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.ByteGraph.ByteGraph
import org.apache.kafka.common.serialization.Serializer

class ByteGraphSerializer extends Serializer[ByteGraph] {
  override def serialize(topic: String, byteGraph: ByteGraph): Array[Byte] = {
    if (byteGraph == null) null
    else ByteGraph.render(byteGraph, withVersionFlag = true).bytes()
  }
}
