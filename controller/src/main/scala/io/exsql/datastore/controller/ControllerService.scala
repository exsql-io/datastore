package io.exsql.datastore.controller

import ControllerServer.ControllerServerConfiguration
import io.exsql.datastore.controller.methods._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, http}
import com.twitter.util.Future
import io.exsql.datastore.controller.kafka.NamespaceHelper
import io.exsql.datastore.controller.methods.{MethodCreateNamespaceExecutor, MethodCreateStreamExecutor, MethodDeployStreamExecutor, MethodDescribeClusterExecutor, MethodDropNamespaceExecutor, MethodDropStreamExecutor, MethodGetNamespaceExecutor, MethodGetStreamExecutor, MethodListNamespaceExecutor, MethodStatusExecutor}
import org.apache.helix.HelixManager

class ControllerService(private val helixManager: HelixManager,
                        private val namespaceHelper: NamespaceHelper,
                        private val controllerServerConfiguration: ControllerServerConfiguration) extends Service[http.Request, http.Response] {

  private val methodCreateNamespaceExecutor = methods.MethodCreateNamespaceExecutor(namespaceHelper)

  private val methodDropNamespaceExecutor = methods.MethodDropNamespaceExecutor(namespaceHelper)

  private val methodHasNamespaceExecutor = methods.MethodGetNamespaceExecutor(namespaceHelper)

  private val methodListNamespaceExecutor = methods.MethodListNamespaceExecutor(namespaceHelper)

  private val methodCreateStreamExecutor = methods.MethodCreateStreamExecutor(namespaceHelper)

  private val methodDropStreamExecutor = methods.MethodDropStreamExecutor(namespaceHelper)

  private val methodDeployStreamExecutor = methods.MethodDeployStreamExecutor(namespaceHelper)

  private val methodGetStreamExecutor = methods.MethodGetStreamExecutor(namespaceHelper)

  private val methodDescribeClusterExecutor = methods.MethodDescribeClusterExecutor(controllerServerConfiguration)

  override def apply(request: Request): Future[Response] = request.path match {
    case Protocol.MethodStatus.Name => MethodStatusExecutor(request)
    case Protocol.MethodCreateNamespace.Name => methodCreateNamespaceExecutor(request)
    case Protocol.MethodDropNamespace.Name => methodDropNamespaceExecutor(request)
    case Protocol.MethodGetNamespace.Name => methodHasNamespaceExecutor(request)
    case Protocol.MethodListNamespaces.Name => methodListNamespaceExecutor(request)
    case Protocol.MethodCreateStream.Name => methodCreateStreamExecutor(request)
    case Protocol.MethodDropStream.Name => methodDropStreamExecutor(request)
    case Protocol.MethodDeployStream.Name => methodDeployStreamExecutor(request)
    case Protocol.MethodGetStream.Name => methodGetStreamExecutor(request)
    case Protocol.MethodDescribeCluster.Name => methodDescribeClusterExecutor(request)
  }

}

object ControllerService {

  def apply(helixManager: HelixManager,
            namespaceStateStoreHelper: NamespaceHelper,
            controllerServerConfiguration: ControllerServerConfiguration): ControllerService = {

    new ControllerService(helixManager, namespaceStateStoreHelper, controllerServerConfiguration)
  }

}