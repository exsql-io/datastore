package io.exsql.datastore.controller.kafka

import java.util.Properties

import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.ByteGraph.ByteGraph
import com.twitter.util.{Future, Promise}
import io.exsql.datastore.controller.Protocol
import org.apache.kafka.clients.producer._

class KafkaProducerHelper(private val producer: Producer[String, ByteGraph]) {

  def createStream(stream: ByteGraph): Future[Boolean] = send(
    s"${KafkaAdminHelper.topicPrefix}-${stream.namespace.as[String]()}", "create-stream", stream.name.as[String](), stream
  )

  def dropStream(namespace: String, name: String): Future[Boolean] = send(
    s"${KafkaAdminHelper.topicPrefix}-$namespace", "drop-stream", name, null
  )

  def deployStream(namespace: String, name: String, deployReplicas: Short): Future[Boolean] = {
    val byteGraph = ByteGraph
      .rowBuilder(Protocol.StreamDefinition)
      .appendString(namespace)
      .appendString(name)
      .appendNull()
      .appendNull()
      .appendNull()
      .appendShort(deployReplicas)
      .build()

    send(s"${KafkaAdminHelper.topicPrefix}-$namespace", "deploy-stream", name, byteGraph)
  }

  def close(): Unit = {
    producer.close()
  }

  private def send(topic: String, action: String, key: String, value: ByteGraph): Future[Boolean] = {
    val result = new Promise[Boolean]()

    val record = new ProducerRecord[String, ByteGraph](topic, key, value)
    record.headers().add("action", action.getBytes("UTF-8"))
    producer.send(record, (_: RecordMetadata, exception: Exception) =>
      if (exception != null) result.raise(exception)
      else result.setValue(true)
    )

    result
  }

}

object KafkaProducerHelper {

  def apply(kafkaConnectionString: String): KafkaProducerHelper = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionString)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.exsql.datastore.controller.kafka.ByteGraphSerializer")

    new KafkaProducerHelper(new KafkaProducer[String, ByteGraph](properties))
  }

}