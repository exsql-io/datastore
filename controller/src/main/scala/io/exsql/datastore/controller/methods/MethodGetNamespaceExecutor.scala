package io.exsql.datastore.controller.methods

import java.util.UUID

import io.exsql.bytegraph.bytes.ByteGraphBytes
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol
import io.exsql.datastore.controller.kafka.NamespaceHelper

case class MethodGetNamespaceExecutor(private val namespaceStateStoreHelper: NamespaceHelper) {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodGetNamespace.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodGetNamespace.requestSchema()).flatMap { byteGraph =>
        getNamespace(byteGraph.body.name.as[String]()).map { namespace =>
          Protocol.httpResponse(
            body = Protocol.MethodGetNamespace.response(
              requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
              namespace = namespace,
              writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
            ),
            contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
            renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
          )
        }
      }
  }

  private def getNamespace(name: String): Future[Option[ByteGraphBytes]] = {
    namespaceStateStoreHelper.get(name).map(_.map(_.byteGraph.bytes.slice(1)))
  }

}
