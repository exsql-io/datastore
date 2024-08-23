package io.exsql.datastore.replica.materialized

import java.nio.file.Path
import com.google.common.primitives.Ints
import io.exsql.bytegraph.bytes.ByteArrayByteGraphBytes
import io.exsql.bytegraph.metadata.ByteGraphSchema
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchema
import io.exsql.bytegraph.{ByteArrayUtils, ByteGraph, Utf8Utils}
import RocksDbManager.ManagedRocksDb
import RocksDbStream._
import com.twitter.concurrent.AsyncStream
import com.twitter.util.Future
import org.rocksdb._

import scala.collection.immutable.ArraySeq

class RocksDbStream(override val namespace: String,
                    override val name: String,
                    override val id: Int,
                    override val schema: ByteGraphSchema,
                    val partition: Int,
                    val directory: Path) extends Stream {

  override val tableSchema: ByteGraphSchema = ByteGraphSchema.rowOf(ArraySeq.unsafeWrapArray(schema.fields) ++ Stream.specialFields: _*)

  private val statistics = StreamStatistics()

  private val managedRocksDb: ManagedRocksDb = RocksDbManager.getOrCreate(directory, namespace)

  private val startPrefix = internalKey(id, partition, "")

  private val endPrefix = internalKey(id, partition + 1, "")

  private val defaultReader = {
    val reader = ByteGraph.reader(schema)
    reader._partition_id = partition
    reader
  }

  override def get(key: String): Future[Option[ByteGraph.ByteGraph]] = Future {
    Option(managedRocksDb.rocksDb.get(managedRocksDb.defaultColumnFamilyHandle, internalKey(id, partition, key)))
      .map(bytes => ByteGraph.valueOf(ByteArrayByteGraphBytes(bytes), Some(schema)))
  }

  override def getAll(keys: Iterator[String]): AsyncStream[Array[AnyRef]] = {
    val reader = ByteGraph.reader(schema)
    reader._partition_id = partition

    AsyncStream.fromSeq(keys.toSeq)
      .mapF(get)
      .filter(_.nonEmpty)
      .map(value => reader.read(value.get.bytes.bytes()))
  }

  override def put(key: String, value: ByteGraph.ByteGraph): Future[Boolean] = Future {
    val writeOptions = new WriteOptions
    writeOptions.setDisableWAL(true)

    val k = internalKey(id, partition, key)
    val v = ByteGraph.render(value, withVersionFlag = true).bytes()
    managedRocksDb.rocksDb.put(managedRocksDb.defaultColumnFamilyHandle, writeOptions, k, v)

    writeOptions.close()

    statistics.rows.increment()
    statistics.bytes.increment((k.length + v.length).toLong)

    true
  }

  override def putAll(entries: Iterator[(String, ByteGraph.ByteGraph)]): Future[Boolean] = Future {
    val writeOptions = new WriteOptions
    writeOptions.setDisableWAL(true)

    val writeBatch = new WriteBatch

    var count = 0L
    var bytes = 0L

    while (entries.hasNext) {
      val (key, value) = entries.next()
      val k = internalKey(id, partition, key)
      val v = ByteGraph.render(value, withVersionFlag = true).bytes()

      writeBatch.put(managedRocksDb.defaultColumnFamilyHandle, k, v)

      count += 1
      bytes += k.length + v.length
    }

    managedRocksDb.rocksDb.write(writeOptions, writeBatch)

    writeBatch.close()
    writeOptions.close()

    statistics.rows.increment(count)
    statistics.bytes.increment(bytes)
    true
  }

  override def remove(key: String): Future[Boolean] = Future {
    val writeOptions = new WriteOptions
    writeOptions.setDisableWAL(true)

    val k = internalKey(id, partition, key)

    var bytes = 0L
    Option(managedRocksDb.rocksDb.get(managedRocksDb.defaultColumnFamilyHandle, k)).foreach { value =>
      bytes += k.length + value.length
    }

    managedRocksDb.rocksDb.singleDelete(managedRocksDb.defaultColumnFamilyHandle, writeOptions, k)

    writeOptions.close()

    statistics.rows.decrement()
    statistics.bytes.decrement(bytes)
    true
  }

  override def removeAll(keys: Iterator[String]): Future[Boolean] = Future {
    val writeOptions = new WriteOptions
    writeOptions.setDisableWAL(true)

    val writeBatch = new WriteBatch

    var count = 0L
    var bytes = 0L

    while (keys.hasNext) {
      val k = internalKey(id, partition, keys.next())
      Option(managedRocksDb.rocksDb.get(managedRocksDb.defaultColumnFamilyHandle, k)).foreach { value =>
        bytes += k.length + value.length
      }

      writeBatch.singleDelete(managedRocksDb.defaultColumnFamilyHandle, k)
      count += 1
    }

    managedRocksDb.rocksDb.write(writeOptions, writeBatch)

    writeBatch.close()
    writeOptions.close()

    statistics.rows.decrement(count)
    statistics.bytes.decrement(bytes)
    true
  }

  override def iterator(): CloseableIterator[ByteGraph.ByteGraph] = {
    new RocksDbIterator(managedRocksDb, startPrefix, endPrefix, schema)
  }

  override def iterator(readSchema: ByteGraphSchema): CloseableIterator[Array[AnyRef]] = {
    iterator().map(value => defaultReader.read(value.bytes.bytes()))
  }

  override def count(): Long = statistics.rows.get()

  override def bytes(): Long = statistics.bytes.get()

}

object RocksDbStream {

  def apply(namespace: String, name: String, id: Int, schema: ByteGraphSchema, partition: Int, directory: Path): RocksDbStream = {
    new RocksDbStream(namespace, name, id, schema, partition, directory)
  }

  private def internalKey(id: Int, partition: Int, key: String): Array[Byte] = {
    ByteArrayUtils.concat(Ints.toByteArray(id), Ints.toByteArray(partition), Utf8Utils.encode(key))
  }

  class RocksDbIterator(private val managedRocksDb: ManagedRocksDb,
                        private val startPrefix: Array[Byte],
                        private val endPrefix: Array[Byte],
                        private val schema: ByteGraphSchema) extends CloseableIterator[ByteGraph.ByteGraph] {

    private val snapshot = managedRocksDb.rocksDb.getSnapshot

    private val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setFillCache(false)
    readOptions.setIterateUpperBound(new Slice(endPrefix))
    readOptions.setSnapshot(snapshot)

    private val rocksIterator = {
      val instance = managedRocksDb.rocksDb.newIterator(managedRocksDb.defaultColumnFamilyHandle, readOptions)
      instance.seek(startPrefix)
      instance
    }

    override def hasNext: Boolean = rocksIterator.isValid

    override def next(): ByteGraph.ByteGraph = {
      val byteGraph = ByteGraph.valueOf(ByteArrayByteGraphBytes(rocksIterator.value()), Some(schema))
      rocksIterator.next()

      byteGraph
    }

    override def close(): Unit = {
      rocksIterator.close()
      readOptions.close()
      managedRocksDb.rocksDb.releaseSnapshot(snapshot)
    }

  }

}
