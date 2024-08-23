package io.exsql.datastore.replica.materialized

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import better.files._
import RocksDbManager._
import org.rocksdb._

import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap

class RocksDbManager(val directory: Path) {

  private val instances = TrieMap.empty[String, ManagedRocksDb]

  def getOrCreate(namespace: String): ManagedRocksDb = this.synchronized {
    if (!instances.contains(namespace)) {
      val file = File(directory)

      val options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setAllowConcurrentMemtableWrite(true)
        .setEnableWriteThreadAdaptiveYield(true)
        .setIncreaseParallelism(Runtime.getRuntime.availableProcessors())
        .setAllowMmapReads(true)
        .setAllowMmapWrites(true)
        .setManualWalFlush(true)

      val blockBasedTableConfig = new BlockBasedTableConfig()
        .setFilterPolicy(new BloomFilter())
        .setIndexType(IndexType.kHashSearch)
        .setFormatVersion(4)
        .setBlockRestartInterval(16)

      val columnFamilyOptions: ColumnFamilyOptions = new ColumnFamilyOptions()
        .setCompressionType(CompressionType.LZ4_COMPRESSION)
        .useFixedLengthPrefixExtractor(Integer.BYTES * 2)
        .setTableFormatConfig(blockBasedTableConfig)
        .setCompactionStyle(CompactionStyle.LEVEL)
        .setLevel0FileNumCompactionTrigger(10)
        .setLevel0SlowdownWritesTrigger(20)
        .setLevel0StopWritesTrigger(40)
        .setMemtablePrefixBloomSizeRatio(0.1)
        .setMaxWriteBufferNumberToMaintain(0)

      val columnFamilyDescriptors = Map(
        rocksDbDefaultColumnFamilyName -> new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions)
      )

      val existingColumnFamilyHandles: java.util.List[ColumnFamilyHandle] = new java.util.ArrayList()
      val rocksDb = RocksDB.open(options, (file / namespace).pathAsString, columnFamilyDescriptors.values.toList.asJava, existingColumnFamilyHandles)

      val columnFamilyHandles = existingColumnFamilyHandles.asScala.map { columnFamilyHandle =>
        new String(columnFamilyHandle.getName, StandardCharsets.UTF_8) -> columnFamilyHandle
      }.toMap

      instances.put(
        namespace,
        new ManagedRocksDb(rocksDb, options, columnFamilyOptions, columnFamilyDescriptors, columnFamilyHandles)
      )
    }

    instances(namespace)
  }

  def close(): Unit = {
    instances.valuesIterator.foreach(_.close())
  }

}

object RocksDbManager {

  private val rocksDbDefaultColumnFamilyName: String = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8)

  private val instances = TrieMap.empty[String, RocksDbManager]

  def init(directory: Path): Unit = this.synchronized {
    if (!instances.contains(directory.toString)) {
      val file = File(directory)
      if (file.exists) {
        file.clear()
        file.delete(swallowIOExceptions = true)
      }

      file.createDirectoryIfNotExists(createParents = true)

      instances += directory.toString -> new RocksDbManager(directory)
    }
  }

  def getOrCreate(directory: Path, namespace: String): ManagedRocksDb = {
    instances(directory.toString).getOrCreate(namespace)
  }

  def close(): Unit = instances.valuesIterator.foreach(_.close())

  class ManagedRocksDb(val rocksDb: RocksDB,
                       private val options: DBOptions,
                       private val columnFamilyOptions: ColumnFamilyOptions,
                       val columnFamilyDescriptors: Map[String, ColumnFamilyDescriptor],
                       val columnFamilyHandles: Map[String, ColumnFamilyHandle]) {

    val defaultColumnFamilyHandle: ColumnFamilyHandle = columnFamilyHandles(rocksDbDefaultColumnFamilyName)

    def close(): Unit = {
      rocksDb.syncWal()
      rocksDb.close()
      options.close()
      columnFamilyOptions.close()
      columnFamilyHandles.valuesIterator.foreach(_.close())
    }

  }

}
