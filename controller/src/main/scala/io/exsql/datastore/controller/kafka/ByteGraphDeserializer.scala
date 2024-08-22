package io.exsql.datastore.controller.kafka

import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.ByteGraph.ByteGraph
import io.exsql.bytegraph.bytes.ByteArrayByteGraphBytes
import io.exsql.datastore.controller.Protocol
import org.apache.kafka.common.serialization.Deserializer

class ByteGraphDeserializer extends Deserializer[ByteGraph] {
  override def deserialize(topic: String, data: Array[Byte]): ByteGraph = {
    if (data == null) null
    else ByteGraph.valueOf(ByteArrayByteGraphBytes(data), Some(Protocol.StreamDefinition))
  }
}
