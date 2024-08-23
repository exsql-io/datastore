package io.exsql.datastore.replica.kafka

import io.exsql.bytegraph.ByteGraph.ByteGraph
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap

object RecordDispatcher {

  private val callbacks: TrieMap[String, TrieMap[Int, Seq[ConsumerRecord[String, ByteGraph]] => Boolean]] = TrieMap.empty

  def register(stream: String, partition: Int)(callback: Seq[ConsumerRecord[String, ByteGraph]] => Boolean): Unit = {
    callbacks.getOrElseUpdate(stream, TrieMap.empty).putIfAbsent(partition, callback)
  }

  def unregister(stream: String, partition: Int): Unit = {
    callbacks.get(stream).foreach(_.remove(partition))
  }

  def dispatch(records: ConsumerRecords[String, ByteGraph]): Unit = {
    val putRecords = records.iterator().asScala.filter { record =>
      val actionHeader = record.headers().headers("action").iterator()
      if (actionHeader.hasNext) new String(actionHeader.next().value(), "UTF-8") == "put"
      else false
    }

    val recordsByStreamPartition = putRecords.toSeq.groupBy { record =>
      record.topic() -> record.partition()
    }

    recordsByStreamPartition.foreach { case ((stream, partition), records) =>
      callbacks.get(stream).flatMap(_.get(partition)).foreach(_(records))
    }
  }

}
