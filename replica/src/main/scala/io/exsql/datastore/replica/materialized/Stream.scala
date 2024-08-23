package io.exsql.datastore.replica.materialized

import com.google.common.primitives.Ints
import CloseableIterator._
import com.twitter.concurrent.AsyncStream
import com.twitter.util.Future
import io.exsql.bytegraph.ByteGraph.ByteGraph
import io.exsql.bytegraph.metadata.ByteGraphSchema
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphSchema, ByteGraphSchemaField}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import io.exsql.datastore.replica.Protocol
import io.exsql.datastore.replica.partitioning.RendezvousHash
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap

import java.io.Closeable
import java.nio.file.Path
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.collection.compat.immutable.ArraySeq
import scala.collection.concurrent.TrieMap
import scala.collection.parallel.CollectionConverters.IterableIsParallelizable

trait CloseableIterator[T] extends Iterator[T] with Closeable
object CloseableIterator {
  implicit class CloseableIteratorWrapper[T](private val wrapped: Iterator[T]) extends CloseableIterator[T] {
    override def hasNext: Boolean = wrapped.hasNext
    override def next(): T = wrapped.next()
    override def close(): Unit = ()
  }
}

case class StreamStatistics(rows: AtomicLongCounter = AtomicLongCounter(),
                            bytes: AtomicLongCounter = AtomicLongCounter())

trait Stream {
  val namespace: String
  val name: String
  val id: Int

  val schema: ByteGraphSchema
  val tableSchema: ByteGraphSchema

  def get(key: String): Future[Option[ByteGraph]]
  def getAll(keys: Iterator[String]): AsyncStream[Array[AnyRef]]
  def putAll(entries: Iterator[(String, ByteGraph)]): Future[Boolean]
  def put(key: String, value: ByteGraph): Future[Boolean]
  def remove(key: String): Future[Boolean]
  def removeAll(keys: Iterator[String]): Future[Boolean]
  def iterator(readSchema: ByteGraphSchema): CloseableIterator[Array[AnyRef]]
  def iterator(): CloseableIterator[ByteGraph]
  def count(): Long
  def bytes(): Long
}

trait StreamTable extends Stream {
  def register(partition: Int)(builder: (String, String, ByteGraphSchema, Int) => Stream): Unit
  def unregister(partition: Int): Unit
  def iterator(readSchema: ByteGraphSchema, partition: Int): CloseableIterator[Array[AnyRef]]
  def iterator(partition: Int): CloseableIterator[ByteGraph]
  def count(partition: Int): Long
  def bytes(partition: Int): Long
}

case class StreamView(namespace: String, name: String, sql: String, schema: ByteGraphSchema)

object Stream {

  private val encoding: String = "UTF-8"

  private val streamIdGenerator = AtomicLongCounter()

  val PartitionIdFieldName = "_partition_id"

  val EncodedAssertedGraphFieldName = "encoded_asserted_graph"

  val specialFields: List[ByteGraphSchemaField] = List(
    ByteGraphSchemaField(PartitionIdFieldName, ByteGraphValueType.Int),
    ByteGraphSchemaField(EncodedAssertedGraphFieldName, ByteGraphValueType.Blob)
  )

  class ProxyStream(override val namespace: String,
                    override val name: String,
                    override val id: Int,
                    override val schema: ByteGraphSchema,
                    val partition: Int,
                    val placement: Map[String, String]) extends Stream {

    override val tableSchema: ByteGraphSchema = ByteGraphSchema.rowOf(ArraySeq.unsafeWrapArray(schema.fields) ++ specialFields: _*)

    override def get(key: String): Future[Option[ByteGraph]] = ???
    override def getAll(keys: Iterator[String]): AsyncStream[Array[AnyRef]] = ???
    override def putAll(entries: Iterator[(String, ByteGraph)]): Future[Boolean] = ???
    override def put(key: String, value: ByteGraph): Future[Boolean] = ???
    override def remove(key: String): Future[Boolean] = ???
    override def removeAll(keys: Iterator[String]): Future[Boolean] = ???
    override def iterator(readSchema: ByteGraphSchema): CloseableIterator[Array[AnyRef]] = ???
    override def iterator(): CloseableIterator[ByteGraph] = ???

    override def count(): Long = {
      val response = Protocol.MethodGetStreamPartitionStatistics.execute(
        s"http://${placement("ONLINE")}",
        s"proxy-stream.${UUID.randomUUID().toString}",
        namespace,
        name,
        partition
      )

      response.body.statistics.rows.as[Long]()
    }

    override def bytes(): Long = {
      val response = Protocol.MethodGetStreamPartitionStatistics.execute(
        s"http://${placement("ONLINE")}",
        s"proxy-stream.${UUID.randomUUID().toString}",
        namespace,
        name,
        partition
      )

      response.body.statistics.selectDynamic("bytes").as[Long]()
    }

  }

  class PartitionedStream(override val namespace: String,
                          override val name: String,
                          override val id: Int,
                          override val schema: ByteGraphSchema) extends StreamTable {

    private val rendezvousHash = RendezvousHash()

    private val streams: TrieMap[Int, Stream] = TrieMap.empty

    override val tableSchema: ByteGraphSchema = ByteGraphSchema.rowOf(ArraySeq.unsafeWrapArray(schema.fields) ++ specialFields: _*)

    override def register(partition: Int)(builder: (String, String, ByteGraphSchema, Int) => Stream): Unit = {
      val stream = builder(namespace, name, schema, partition)
      streams.put(partition, stream)

      if (!stream.isInstanceOf[ProxyStream]) rendezvousHash.add(Ints.toByteArray(partition))
    }

    override def unregister(partition: Int): Unit = {
      rendezvousHash.remove(Ints.toByteArray(partition))
      streams.remove(partition)
    }

    override def get(key: String): Future[Option[ByteGraph]] = {
      Future(Ints.fromByteArray(rendezvousHash.get(key.getBytes(encoding)))).flatMap { partition =>
        streams(partition).get(key)
      }
    }

    override def getAll(keys: Iterator[String]): AsyncStream[Array[AnyRef]] = {
      AsyncStream(keys.to(LazyList).groupBy(key => Ints.fromByteArray(rendezvousHash.get(key.getBytes(encoding)))).toSeq)
        .flatMap { partitions =>
          val values: Seq[AsyncStream[Array[AnyRef]]] = partitions.map { case (partition, keys) =>
            streams(partition).getAll(keys.iterator)
          }

          AsyncStream.merge(values: _*)
        }
    }

    override def put(key: String, value: ByteGraph): Future[Boolean] = {
      Future(Ints.fromByteArray(rendezvousHash.get(key.getBytes(encoding)))).flatMap { partition =>
        streams(partition).put(key, value)
      }
    }

    override def putAll(entries: Iterator[(String, ByteGraph)]): Future[Boolean] = {
      val results: Iterable[Future[Boolean]] = entries.to(LazyList).groupBy { case (key, _) =>
        Ints.fromByteArray(rendezvousHash.get(key.getBytes(encoding)))
      }.map { case (partition, partitionEntries) =>
        streams(partition).putAll(partitionEntries.iterator)
      }

      Future
        .collect(results.toSeq)
        .map(_.forall(_ == true))
    }

    override def remove(key: String): Future[Boolean] = {
      Future(Ints.fromByteArray(rendezvousHash.get(key.getBytes(encoding)))).flatMap { partition =>
        streams(partition).remove(key)
      }
    }

    override def removeAll(keys: Iterator[String]): Future[Boolean] = {
      val results: Iterable[Future[Boolean]] = keys.to(LazyList).groupBy { key =>
        Ints.fromByteArray(rendezvousHash.get(key.getBytes(encoding)))
      }.map { case (partition, partitionKeys) =>
        streams(partition).removeAll(partitionKeys.iterator)
      }

      Future
        .collect(results.toSeq)
        .map(_.forall(_ == true))
    }

    override def iterator(readSchema: ByteGraphSchema): CloseableIterator[Array[AnyRef]] = {
      streams.valuesIterator.flatMap(_.iterator(readSchema))
    }

    override def iterator(readSchema: ByteGraphSchema, partition: Int): CloseableIterator[Array[AnyRef]] = {
      streams(partition).iterator(readSchema)
    }

    override def iterator(): CloseableIterator[ByteGraph] = {
      streams.valuesIterator.flatMap(_.iterator())
    }

    override def iterator(partition: Int): CloseableIterator[ByteGraph] = {
      streams(partition).iterator()
    }

    override def count(): Long = {
      streams.values.par.map(_.count()).sum
    }

    override def count(partition: Int): Long = {
      streams(partition).count()
    }

    override def bytes(): Long = {
      streams.values.par.map(_.bytes()).sum
    }

    override def bytes(partition: Int): Long = {
      streams(partition).bytes()
    }

  }

  class InMemoryStream(override val namespace: String,
                       override val name: String,
                       override val id: Int,
                       override val schema: ByteGraphSchema,
                       val partition: Int) extends Stream {

    private val map: Object2ObjectLinkedOpenHashMap[String, ByteGraph] = new Object2ObjectLinkedOpenHashMap[String, ByteGraph]()

    private val statistics = StreamStatistics()

    override val tableSchema: ByteGraphSchema = ByteGraphSchema.rowOf(ArraySeq.unsafeWrapArray(schema.fields) ++ specialFields: _*)

    override def get(key: String): Future[Option[ByteGraph]] = Future(Option(map.get(key)))

    override def getAll(keys: Iterator[String]): AsyncStream[Array[AnyRef]] = {
      val reader = ByteGraph.reader(schema)
      reader._partition_id = partition

      AsyncStream.fromSeq(keys.toSeq).map { key =>
        Option(map.get(key)).map(value => reader.read(value.bytes.bytes()))
      }.filter(_.nonEmpty).map(_.get)
    }

    override def put(key: String, value: ByteGraph): Future[Boolean] = Future {
      map.put(key, value)

      statistics.rows.increment()
      statistics.bytes.increment((key.getBytes("UTF-8").length + value.bytes.length()).toLong)

      true
    }

    override def putAll(entries: Iterator[(String, ByteGraph)]): Future[Boolean] = Future {
      val asMap = entries.toMap
      map.putAll(asMap.asJava)

      statistics.rows.increment(asMap.size.toLong)

      val iterator = asMap.iterator
      while (iterator.hasNext) {
        val (key, value) = iterator.next()
        statistics.bytes.increment((key.getBytes("UTF-8").length + value.bytes.length()).toLong)
      }

      true
    }

    override def remove(key: String): Future[Boolean] = Future {
      val removed = Option(map.remove(key))
      removed.foreach { byteGraph =>
        statistics.rows.decrement()
        statistics.bytes.decrement((key.getBytes("UTF-8").length + byteGraph.bytes.length()).toLong)
      }

      removed.isDefined
    }

    override def removeAll(keys: Iterator[String]): Future[Boolean] = Future {
      var count = 0L
      var bytes = 0L

      while (keys.hasNext) {
        val key = keys.next()
        Option(map.remove(key)).foreach { byteGraph =>
          count += 1
          bytes += (key.getBytes("UTF-8").length + byteGraph.bytes.length()).toLong
        }
      }

      statistics.rows.decrement(count)
      statistics.bytes.decrement(bytes)

      count > 0
    }

    override def iterator(readSchema: ByteGraphSchema): CloseableIterator[Array[AnyRef]] =  {
      val reader = ByteGraph.reader(readSchema)//ByteGraph.partialReader(readSchema, tableSchema)
      reader._partition_id = partition
      map.values().iterator().asScala.map(byteGraph => reader.read(byteGraph.bytes.slice(4).bytes()))
    }

    override def iterator(): CloseableIterator[ByteGraph] =  {
      map.values().iterator().asScala
    }

    override def count(): Long = {
      statistics.rows.get()
    }

    override def bytes(): Long = {
      statistics.bytes.get()
    }

  }

  def inMemory(namespace: String, name: String, schema: ByteGraphSchema, partition: Int): Stream = {
    new InMemoryStream(namespace, name, streamIdGenerator.incrementAndGet().toInt, schema, partition)
  }

  def onDisk(namespace: String, name: String, schema: ByteGraphSchema, partition: Int, directory: Path): Stream = {
    new RocksDbStream(namespace, name, streamIdGenerator.incrementAndGet().toInt, schema, partition, directory)
  }

  def proxy(namespace: String,
            name: String,
            schema: ByteGraphSchema,
            partition: Int,
            placement: Map[String, String]): Stream = {

    new ProxyStream(namespace, name, streamIdGenerator.incrementAndGet().toInt, schema, partition, placement)
  }

  def partitioned(namespace: String, name: String, schema: ByteGraphSchema): StreamTable = {
    new PartitionedStream(namespace, name, streamIdGenerator.incrementAndGet().toInt, schema)
  }

}