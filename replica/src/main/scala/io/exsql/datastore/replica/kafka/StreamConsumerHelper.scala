package io.exsql.datastore.replica.kafka

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.FSM.Shutdown
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Timers}
import akka.pattern.ask
import akka.util.Timeout
import com.google.common.collect.Sets
import io.exsql.bytegraph.ByteGraph.ByteGraph
import StreamConsumerHelper.{Register, Unregister}
import io.exsql.datastore.replica.Protocol
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, duration}

class StreamConsumerHelper(val id: String, private val consumerActor: ActorRef) {

  private val timeout = scala.concurrent.duration.Duration(3600, TimeUnit.SECONDS)

  implicit private val askTimeout: Timeout = Timeout(timeout)

  def register(stream: String, partition: Int)
              (callback: Seq[ConsumerRecord[String, ByteGraph]] => Boolean): Boolean = {

    Await.result((consumerActor ? Register(stream, partition, callback)).mapTo[Boolean], timeout)
  }

  def unregister(stream: String, partition: Int): Boolean = {
    Await.result((consumerActor ? Unregister(stream, partition)).mapTo[Boolean], timeout)
  }

}

object StreamConsumerHelper {

  def apply(id: String, kafkaConnectionString: String, actorSystem: ActorSystem): StreamConsumerHelper = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionString)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, id)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.exsql.datastore.replica.kafka.ByteGraphDeserializer")
    properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false")
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000")
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

    new StreamConsumerHelper(id, actorSystem.actorOf(Props(new KafkaConsumerActor(id, properties))))
  }

  final case class Register(stream: String, partition: Int, callback: Seq[ConsumerRecord[String, ByteGraph]] => Boolean)
  final case class Unregister(stream: String, partition: Int)
  final case object Poll

  private class KafkaConsumerActor(private val id: String,
                                   private val properties: Properties) extends Actor with Timers {

    override def receive: Receive = waitForRegistration()

    private def waitForRegistration(): Receive = {
      case register: Register =>
        context.become(ready(new KafkaConsumer[String, ByteGraph](properties)))
        timers.startTimerWithFixedDelay("polling", Poll, duration.Duration(100, TimeUnit.MILLISECONDS))
        self.tell(register, sender())

      case _: Unregister => ()
      case Poll => ()
      case Stop | Shutdown | PoisonPill =>
        context.stop(self)
    }

    private def ready(consumer: Consumer[String, ByteGraph]): Receive = {
      case Register(stream, partition, callback) =>
        register(consumer, stream, partition, callback)
        sender() ! true

      case Unregister(stream, partition) =>
        unregister(consumer, stream, partition)
        if (consumer.assignment().isEmpty) {
          timers.cancel("polling")
          consumer.close()
          context.become(waitForRegistration())
        }

        sender() ! true

      case Poll => RecordDispatcher.dispatch(consumer.poll(Duration.ofMillis(10)))
      case Stop | Shutdown | PoisonPill =>
        timers.cancel("polling")
        consumer.close()
        context.stop(self)
    }

    private def register(consumer: Consumer[String, ByteGraph],
                         stream: String,
                         partition: Int,
                         callback: Seq[ConsumerRecord[String, ByteGraph]] => Boolean): Unit = {

      val topic = s"${Protocol.TopicPrefix}-$stream"

      RecordDispatcher.register(topic, partition)(callback)

      val topicPartition = new TopicPartition(topic, partition)
      val assignment = Sets.newHashSet[TopicPartition]()
      assignment.addAll(consumer.assignment())
      assignment.add(topicPartition)

      consumer.assign(assignment)
      consumer.seekToBeginning(Seq(topicPartition).asJavaCollection)
    }

    private def unregister(consumer: Consumer[String, ByteGraph], stream: String, partition: Int): Unit = {
      val topic = s"${Protocol.TopicPrefix}-$stream"
      val topicPartition = new TopicPartition(topic, partition)
      val assignment = Sets.newHashSet[TopicPartition]()
      assignment.addAll(consumer.assignment())
      assignment.remove(topicPartition)
      consumer.assign(assignment)

      RecordDispatcher.unregister(topic, partition)
    }

  }

}
