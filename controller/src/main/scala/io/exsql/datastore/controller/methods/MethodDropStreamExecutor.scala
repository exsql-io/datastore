package io.exsql.datastore.controller.methods

import java.util.UUID
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol
import io.exsql.datastore.controller.kafka.NamespaceHelper

case class MethodDropStreamExecutor(private val namespaceHelper: NamespaceHelper) {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodDropStream.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodDropStream.requestSchema()).flatMap { byteGraph =>
        dropStream(
          byteGraph.body.namespace.as[String](),
          byteGraph.body.name.as[String]()
        ).map { success =>
          Protocol.httpResponse(
            body = Protocol.MethodDropStream.response(
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

  private def dropStream(namespace: String, name: String): Future[Boolean] = {
    namespaceHelper.dropStream(namespace, name)
  }

}
