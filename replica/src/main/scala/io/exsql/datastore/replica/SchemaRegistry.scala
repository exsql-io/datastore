package io.exsql.datastore.replica

import java.util.UUID
import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.metadata.ByteGraphSchema
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchema

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.ArraySeq

private class SchemaRegistry(private val controllerHelper: ControllerHelper) {

  private val schema: TrieMap[String, ByteGraphSchema] = TrieMap.empty

  def getSchemaForTopic(topic: String): ByteGraphSchema = {
    schema.getOrElseUpdate(topic, getSchema(topic))
  }

  def getSchemaForStream(stream: String): ByteGraphSchema = {
    val topic = s"${Protocol.TopicPrefix}-$stream"
    schema.getOrElseUpdate(topic, getSchema(topic))
  }

  private def getSchema(topic: String): ByteGraphSchema = {
    val (namespace, stream) = (topic.replace(s"${Protocol.TopicPrefix}-", "").split('.').toList: @unchecked) match {
      case ns::s::_ => ns -> s
    }

    val response = Protocol.MethodGetStream.execute(
      controllerHelper.find(),
      s"replica-${controllerHelper.id}.${UUID.randomUUID().toString}",
      namespace,
      stream
    )

    ByteGraphSchema.fromJson(ByteGraph.toJsonString(response.body.selectDynamic("schema")))
  }

}

object SchemaRegistry {

  private var instance: SchemaRegistry = _

  def init(): Unit = this.synchronized {
    instance = new SchemaRegistry(ControllerHelper())
  }

  def getSchemaForTopic(topic: String): ByteGraphSchema = { instance.getSchemaForTopic(topic) }

  def getSchemaForStream(stream: String): ByteGraphSchema = { instance.getSchemaForStream(stream) }

  def getTableSchemaForStream(namespace: String, stream: String): ByteGraphSchema = {
    val schema = getSchemaForStream(s"$namespace.$stream")
    ByteGraphSchema.rowOf(ArraySeq.unsafeWrapArray(schema.fields) ++ materialized.Stream.specialFields: _*)
  }

}
