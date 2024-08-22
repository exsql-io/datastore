package io.exsql.datastore.controller.methods

import java.util.UUID

import io.exsql.bytegraph.bytes.ByteGraphBytes
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol
import io.exsql.datastore.controller.kafka.NamespaceHelper

case class MethodListNamespaceExecutor(private val namespaceStateStoreHelper: NamespaceHelper) {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodListNamespaces.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodListNamespaces.requestSchema()).flatMap { byteGraph =>
        listNamespaces().map { namespaces =>
          Protocol.httpResponse(
            body = Protocol.MethodListNamespaces.response(
              requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
              namespaces = namespaces,
              writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
            ),
            contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
            renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
          )
        }
      }
  }

  private def listNamespaces(): Future[Iterator[ByteGraphBytes]] = {
    namespaceStateStoreHelper.list().map(iterator => iterator.map(_._2.byteGraph.bytes))
  }

}
