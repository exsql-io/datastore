package io.exsql.datastore.controller.kafka

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.FSM.Shutdown
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Stash, Timers}
import akka.pattern.ask
import akka.util.Timeout
import io.exsql.bytegraph.ByteGraph.ByteGraph
import KafkaAdminHelper.topicPrefix
import KafkaConsumerHelper.{GetStreams, Register, Unregister}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, Future, duration}

class KafkaConsumerHelper(private val kafkaConsumerActor: ActorRef) {

  private val oneHour = scala.concurrent.duration.Duration(3600, TimeUnit.SECONDS)

  implicit private val askTimeout: Timeout = new Timeout(oneHour)

  def getStreams(namespace: String): Array[NamespaceHelper.Stream] = {
    val streams: Future[Array[NamespaceHelper.Stream]] = (kafkaConsumerActor ? GetStreams(namespace))
      .mapTo[Array[NamespaceHelper.Stream]]

    Await.result(streams, oneHour)
  }

  def register(namespace: String): Unit = {
    kafkaConsumerActor ! Register(namespace)
  }

  def unregister(namespace: String): Unit = {
    kafkaConsumerActor ! Unregister(namespace)
  }

  def close(): Unit = {
    kafkaConsumerActor ! Stop
  }

}

object KafkaConsumerHelper {

  def apply(id: String,
            kafkaAdminHelper: KafkaAdminHelper,
            actorSystem: ActorSystem = ActorSystem("reactive-database")): KafkaConsumerHelper = {

    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAdminHelper.kafkaConnectionString)
    properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, id)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.exsql.datastore.controller.kafka.ByteGraphDeserializer")

    new KafkaConsumerHelper(actorSystem.actorOf(Props(new KafkaConsumerActor(kafkaAdminHelper, properties))))
  }

  final case class Register(namespace: String)
  final case class Unregister(namespace: String)
  final case class GetStreams(namespace: String)
  final case class IsReady(namespaceTopics: Set[String])
  final case object Poll

  private class KafkaConsumerActor(private val kafkaAdminHelper: KafkaAdminHelper,
                                   private val properties: Properties) extends Actor with Timers with Stash {

    private val existingNamespaceTopics = kafkaAdminHelper
      .listNamespaces()
      .filterNot(_.name().contains('.'))
      .map(_.name()).toSet

    private val streams: TrieMap[String, TrieMap[String, NamespaceHelper.Stream]] = TrieMap.empty

    private var isReady: Boolean = false

    private val consumer = {
      val instance = new KafkaConsumer[String, ByteGraph](properties)

      val partitions = existingNamespaceTopics.map { namespace =>
        new TopicPartition(namespace, 0)
      }

      instance.assign(partitions.asJavaCollection)
      instance.seekToBeginning(partitions.asJavaCollection)
      poll(instance, existingNamespaceTopics)
      timers.startTimerWithFixedDelay("polling", Poll, duration.Duration(100, TimeUnit.MILLISECONDS))

      instance
    }

    override def receive: Receive = waitForReadiness()

    def waitForReadiness(): Receive = {
      case _: Register => stash()
      case _: Unregister => stash()
      case _: GetStreams => stash()
      case Poll => poll(consumer, existingNamespaceTopics)
      case IsReady(namespaceTopics) =>
        context.become(subscribedTo(namespaceTopics))
        unstashAll()
    }

    def subscribedTo(namespaceTopics: Set[String]): Receive = {
      case Register(namespace) =>
        val topic = s"$topicPrefix-$namespace"
        val assignments = namespaceTopics ++ Set(topic)

        consumer.assign(assignments.map(topicName => new TopicPartition(topicName, 0)).asJavaCollection)
        consumer.seekToBeginning(Seq(new TopicPartition(topic, 0)).asJavaCollection)
        context.become(subscribedTo(assignments))

      case Unregister(namespace) =>
        val topic = s"$topicPrefix-$namespace"
        val assignments = namespaceTopics -- Set(topic)

        consumer.assign(assignments.map(topicName => new TopicPartition(topicName, 0)).asJavaCollection)
        context.become(subscribedTo(assignments))

      case GetStreams(namespace) =>
        sender() ! streams(namespace).values.toArray

      case Poll => poll(consumer, namespaceTopics)
      case Stop | Shutdown | PoisonPill =>
        timers.cancel("polling")
        consumer.close()
        context.stop(self)
    }

    private def poll(consumer: Consumer[String, ByteGraph], namespaceTopics: Set[String]): Unit = {
      if (consumer.assignment().isEmpty) return

      val records = consumer.poll(Duration.ofMillis(10))
      namespaceTopics.foreach { topic =>
        val namespace = topic.replace(s"${KafkaAdminHelper.topicPrefix}-", "")
        if (!streams.contains(namespace)) {
          streams += namespace -> TrieMap.empty[String, NamespaceHelper.Stream]
        }

        records.records(topic).asScala.foreach { record =>
          val action = new String(record.headers().headers("action").iterator().next().value(), "UTF-8")
          val name = record.key()
          val byteGraph = record.value()

          action match {
            case "create-stream" => streams(namespace).put(name, NamespaceHelper.Stream.fromByteGraph(byteGraph))
            case "drop-stream" => streams(namespace).remove(name)
            case "deploy-stream" =>
              val stream = streams(namespace)(name)
              streams(namespace).replace(
                name,
                stream.copy(deployReplicas = Some(byteGraph.deployReplicas.as[Short]()))
              )
          }
        }
      }

      if (!isReady && records.count() > 0) {
        isReady = true
        self ! IsReady(namespaceTopics)
      }
    }

  }

}