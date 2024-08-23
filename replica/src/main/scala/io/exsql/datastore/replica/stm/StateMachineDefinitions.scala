package io.exsql.datastore.replica.stm

import java.nio.file.Path

import akka.actor.ActorSystem
import io.exsql.datastore.replica.ReplicaServer.ReplicaServerConfiguration
import com.twitter.util.{Await, Future}
import io.exsql.datastore.replica.{NamespaceRegistry, SchemaRegistry}
import io.exsql.datastore.replica.kafka.StreamConsumerHelper
import org.apache.helix.NotificationContext
import org.apache.helix.model.Message
import org.apache.helix.participant.statemachine.{StateModel, StateModelFactory, StateModelInfo, Transition}
import org.slf4j.LoggerFactory

object StateMachineDefinitions {

  private val logger = LoggerFactory.getLogger("io.exsql.datastore.replica.stm.StateMachineDefinitions")

  object ReplicaDefinition {

    val Name: String = "ReactiveDatabaseReplica"

    def factory(configuration: ReplicaServerConfiguration, actorSystem: ActorSystem): StateModelFactory[ReactiveDatabaseReplicaStateModel] = {
      new ReactiveDatabaseReplicaStateModelFactory(configuration, actorSystem)
    }

    class ReactiveDatabaseReplicaStateModelFactory(private val configuration: ReplicaServerConfiguration,
                                                   private val actorSystem: ActorSystem) extends StateModelFactory[ReactiveDatabaseReplicaStateModel] {

      private val streamConsumerHelper = StreamConsumerHelper(
        configuration.id, configuration.kafkaConnectionString, actorSystem
      )

      override def createNewStateModel(resourceName: String, partitionName: String): ReactiveDatabaseReplicaStateModel = {
        new ReactiveDatabaseReplicaStateModel(
          streamConsumerHelper, resourceName, partitionName.split('_').last.toInt, configuration.store, configuration.inMemory
        )
      }

    }

    @StateModelInfo(initialState = "OFFLINE", states = Array("OFFLINE", "ONLINE"))
    class ReactiveDatabaseReplicaStateModel(private val kafkaConsumerHelper: StreamConsumerHelper,
                                            private val stream: String,
                                            private val partition: Int,
                                            private val directory: Path,
                                            private val inMemory: Boolean) extends StateModel {

      @Transition(from = "OFFLINE", to = "ONLINE")
      def onBecomeOnlineFromOffline(message: Message, context: NotificationContext): Unit = {
        logger.info(s"${kafkaConsumerHelper.id} transitioning from ${message.getFromState} to ${message.getToState} for $stream#$partition")

        val (namespace, name) = (stream.split('.').toList: @unchecked) match {
          case ns::s::_ => ns -> s
        }

        NamespaceRegistry.register(namespace, inMemory)
        NamespaceRegistry.get(namespace).register(name, SchemaRegistry.getSchemaForStream(stream), partition, directory)

        val materialized = NamespaceRegistry.get(namespace).get(name)
        kafkaConsumerHelper.register(stream, partition) { records =>
          val (put, remove) = records.partition(_.value() != null)
          val result = Future.collect(Seq(
            materialized.putAll(put.map(record => record.key() -> record.value()).iterator),
            materialized.removeAll(remove.map(_.key()).iterator)
          ))

          Await.result(result.map(_.forall(_ == true)))
        }
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      def onBecomeOfflineFromOnline(message: Message, context: NotificationContext): Unit = {
        logger.info(s"${kafkaConsumerHelper.id} transitioning from ${message.getFromState} to ${message.getToState} for  $stream#$partition")
        kafkaConsumerHelper.unregister(stream, partition)

        val (namespace, name) = (stream.split('.').toList: @unchecked) match {
          case ns::s::_ => ns -> s
        }

        NamespaceRegistry.get(namespace).unregister(name, partition)
      }

      @Transition(from = "OFFLINE", to = "DROPPED")
      def onBecomeDroppedFromOffline(message: Message, context: NotificationContext): Unit = {
        logger.info(s"${kafkaConsumerHelper.id} transitioning from ${message.getFromState} to ${message.getToState} for  $stream#$partition")
      }

    }

  }

}
