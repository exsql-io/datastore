package io.exsql.datastore.controller.stm

import org.apache.helix.HelixDefinedState
import org.apache.helix.model.IdealState.RebalanceMode
import org.apache.helix.model.MasterSlaveSMD.States
import org.apache.helix.model.StateModelDefinition

object StateMachineDefinitions {

  object ReplicaDefinition {

    val Name: String = "ReactiveDatabaseReplica"

    val StateModelFactoryName: String = s"${Name}StateModelFactory"

    val AssignmentStrategyName: String = RebalanceMode.SEMI_AUTO.name()

    val Definition: StateModelDefinition = {
      val builder = new StateModelDefinition.Builder(Name)
      // init state
      builder.initialState(States.OFFLINE.name)

      // add states
      builder.addState(States.MASTER.name, 0)
      builder.addState(States.SLAVE.name, 1)
      builder.addState(States.OFFLINE.name, 2)
      for (state <- HelixDefinedState.values) {
        builder.addState(state.name)
      }

      // add transitions
      builder.addTransition(States.MASTER.name, States.SLAVE.name, 0)
      builder.addTransition(States.SLAVE.name, States.MASTER.name, 1)
      builder.addTransition(States.OFFLINE.name, States.SLAVE.name, 2)
      builder.addTransition(States.SLAVE.name, States.OFFLINE.name, 3)
      builder.addTransition(States.OFFLINE.name, HelixDefinedState.DROPPED.name)

      // bounds
      builder.upperBound(States.MASTER.name, 1)
      builder.dynamicUpperBound(States.SLAVE.name, "R")

      builder.build()
    }

  }

}
