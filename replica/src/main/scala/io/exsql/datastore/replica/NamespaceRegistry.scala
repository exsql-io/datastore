package io.exsql.datastore.replica

import java.util.UUID

import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.ByteGraph.{ByteGraphList, ByteGraphStructure}
import io.exsql.bytegraph.metadata.ByteGraphSchema
import io.exsql.datastore.replica.materialized.{Namespace, StreamView}

import scala.collection.concurrent.TrieMap

class NamespaceRegistry {

  private val namespaces: TrieMap[String, Namespace] = TrieMap.empty

  def register(namespace: String, inMemory: Boolean): Unit = {
    namespaces.putIfAbsent(namespace, Namespace(namespace, inMemory))
  }

  def unregister(namespace: String): Unit = {
    namespaces.remove(namespace)
  }

  def get(namespace: String): Namespace = {
    namespaces(namespace)
  }

}

object NamespaceRegistry {

  private val instance: NamespaceRegistry = new NamespaceRegistry()

  def init(inMemory: Boolean): Unit = instance.synchronized {
    val controllerHelper = ControllerHelper()
    val response = Protocol.MethodListNamespaces.execute(
      controllerHelper.find(),
      s"replica-${controllerHelper.id}-${UUID.randomUUID().toString}"
    )

    val namespaces = response.body.as[ByteGraphList]().iterator
    while (namespaces.hasNext) {
      val namespace = namespaces.next()
      instance.register(namespace.name.as[String](), inMemory)

      val streams = namespace.streams.as[ByteGraphList]().iterator
      while (streams.hasNext) {
        val stream = streams.next()
        val placement: Map[Int, Map[String, String]] = stream.placement.as[ByteGraphStructure]().iterator
          .map { case (id, partition) =>
            val hosts = partition.as[ByteGraphStructure]()
            id.toInt -> hosts.iterator.map { case (status, endpoint) =>
              status -> endpoint.as[String]()
            }.toMap
          }
          .toMap

        instance.get(namespace.name.as[String]()).proxy(
          stream.name.as[String](),
          ByteGraphSchema.fromJson(ByteGraph.toJsonString(stream.selectDynamic("schema"))),
          placement
        )
      }

      val sqlViews = namespace.sqlViews.as[ByteGraphList]().iterator
      while (sqlViews.hasNext) {
        val sqlView = sqlViews.next()
        instance.get(namespace.name.as[String]()).registerView(StreamView(
          sqlView.namespace.as[String](),
          sqlView.name.as[String](),
          sqlView.sql.as[String](),
          ByteGraphSchema.fromJson(ByteGraph.toJsonString(sqlView.selectDynamic("schema")))
        ))
      }
    }
  }

  def register(namespace: String, inMemory: Boolean): Unit = instance.synchronized {
    instance.register(namespace, inMemory)
  }

  def unregister(namespace: String): Unit = instance.synchronized {
    instance.unregister(namespace)
  }

  def get(namespace: String): Namespace = {
    instance.get(namespace)
  }

  def list(): Set[String] = instance.namespaces.keys.toSet

}