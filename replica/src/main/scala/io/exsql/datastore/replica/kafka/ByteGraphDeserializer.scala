package io.exsql.datastore.replica.kafka

import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.ByteGraph.ByteGraph
import io.exsql.bytegraph.bytes.ByteArrayByteGraphBytes
import io.exsql.datastore.replica.SchemaRegistry
import org.apache.kafka.common.serialization.Deserializer

class ByteGraphDeserializer extends Deserializer[ByteGraph] {
  override def deserialize(topic: String, bytes: Array[Byte]): ByteGraph = {
    if (bytes == null) null
    else ByteGraph.valueOf(ByteArrayByteGraphBytes(bytes), Some(SchemaRegistry.getSchemaForTopic(topic)))
  }
}
