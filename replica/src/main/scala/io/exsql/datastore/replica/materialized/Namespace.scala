package io.exsql.datastore.replica.materialized

import java.nio.file.Path

import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchema

import scala.collection.concurrent.TrieMap

class Namespace(val name: String, val inMemory: Boolean) {

  private val streams: TrieMap[String, StreamTable] = TrieMap.empty

  private val views: TrieMap[String, StreamView] = TrieMap.empty

  def register(stream: String, schema: ByteGraphSchema, partition: Int, directory: Path): Unit = streams.synchronized {
    if (!streams.contains(stream)) streams.putIfAbsent(stream, Stream.partitioned(name, stream, schema))

    streams(stream).register(partition)(if (inMemory) Stream.inMemory else Stream.onDisk(_, _, _, _, directory))
  }

  def proxy(stream: String, schema: ByteGraphSchema, placements: Map[Int, Map[String, String]]): Unit = streams.synchronized {
    if (!streams.contains(stream)) {
      val instance = Stream.partitioned(name, stream, schema)
      placements.foreach { case (partition, placement) =>
        instance.register(partition)(Stream.proxy(_, _, _, _, placement))
      }

      streams.putIfAbsent(stream, instance)
    }
  }

  def unregister(stream: String, partition: Int): Unit = {
    if (streams.contains(stream)) streams(stream).unregister(partition)
  }

  def get(stream: String): StreamTable = streams(stream)

  def list(): Map[String, StreamTable] = streams.toMap

  def registerView(view: StreamView): Unit = views.synchronized {
    if (!views.contains(view.name)) views.putIfAbsent(view.name, view)
  }

  def unregisterView(name: String): Unit = views.synchronized {
    views.remove(name)
  }

  def listViews(): Map[String, StreamView] = views.toMap

}

object Namespace {

  def apply(name: String, inMemory: Boolean): Namespace = new Namespace(name, inMemory)

}