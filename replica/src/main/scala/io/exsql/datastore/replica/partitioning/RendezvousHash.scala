package io.exsql.datastore.replica.partitioning

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListSet

import io.exsql.bytegraph.ByteArrayUtils
import net.openhft.hashing.LongHashFunction

import scala.jdk.CollectionConverters._

class RendezvousHash(private val nodeIds: ConcurrentSkipListSet[ByteBuffer] = new ConcurrentSkipListSet[ByteBuffer]()) {

  def remove(nodeId: Array[Byte]): Boolean = {
    nodeIds.remove(ByteBuffer.wrap(nodeId))
  }

  def add(nodeId: Array[Byte]): Boolean = {
    nodeIds.add(ByteBuffer.wrap(nodeId))
  }

  def get(key: Array[Byte]): Array[Byte] = {
    var maxValue: Long = Long.MinValue
    var max: Option[Array[Byte]] = None

    val iterator = nodeIds.iterator()
    while (iterator.hasNext) {
      val nodeId = iterator.next()
      val hash = LongHashFunction.xx().hashBytes(
        ByteArrayUtils.concat(key, nodeId.array())
      )

      if (hash > maxValue) {
        maxValue = hash
        max = Some(nodeId.array())
      }
    }

    max.get
  }

  def size(): Int = {
    nodeIds.size()
  }

  def entries(): Iterator[ByteBuffer] = {
    nodeIds.iterator().asScala
  }

  def clear(): Unit = nodeIds.synchronized {
    nodeIds.clear()
  }

}

object RendezvousHash {

  def apply(): RendezvousHash = new RendezvousHash()

  def apply(nodeIds: Seq[Array[Byte]]): RendezvousHash = {
    val orderedNodeIds = new ConcurrentSkipListSet[ByteBuffer]()
    nodeIds.foreach(bytes => orderedNodeIds.add(ByteBuffer.wrap(bytes)))

    new RendezvousHash(orderedNodeIds)
  }

}