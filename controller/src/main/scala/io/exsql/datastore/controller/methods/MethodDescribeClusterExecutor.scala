package io.exsql.datastore.controller.methods

import java.util.UUID

import io.exsql.datastore.controller.ControllerServer.ControllerServerConfiguration
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol

case class MethodDescribeClusterExecutor(private val controllerServerConfiguration: ControllerServerConfiguration) {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodDescribeCluster.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodDescribeCluster.requestSchema()).map { byteGraph =>
        Protocol.httpResponse(
          body = Protocol.MethodDescribeCluster.response(
            requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
            Array[AnyRef](
              controllerServerConfiguration.cluster,
              Array[Array[AnyRef]](Array[AnyRef](
                controllerServerConfiguration.id,
                controllerServerConfiguration.host,
                java.lang.Integer.valueOf(controllerServerConfiguration.port),
                controllerServerConfiguration.managementEndpointPort.map(java.lang.Integer.valueOf).orNull
              )),
              controllerServerConfiguration.zookeeperConnectionString,
              controllerServerConfiguration.kafkaConnectionString
            ),
            writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true),
          ),
          contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
          renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
        )
      }
  }

}
