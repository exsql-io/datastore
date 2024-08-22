package io.exsql.datastore.controller.methods

import java.util.UUID
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol
import io.exsql.datastore.controller.kafka.NamespaceHelper

case class MethodCreateNamespaceExecutor(private val namespaceHelper: NamespaceHelper) {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodCreateNamespace.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodCreateNamespace.requestSchema()).flatMap { byteGraph =>
        createNamespace(byteGraph.body.name.as[String](), byteGraph.body.replicas.as[Short]()).map { success =>
          Protocol.httpResponse(
            body = Protocol.MethodCreateNamespace.response(
              requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
              success = success,
              writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
            ),
            contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
            renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
          )
        }
      }
  }

  private def createNamespace(name: String, replicas: Short): Future[Boolean] = {
    namespaceHelper.create(name, replicas)
  }

}
