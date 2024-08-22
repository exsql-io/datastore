package io.exsql.datastore.controller.methods

import java.util.UUID
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol
import io.exsql.datastore.controller.kafka.NamespaceHelper

case class MethodGetStreamExecutor(private val namespaceHelper: NamespaceHelper) {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodGetStream.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodGetStream.requestSchema()).flatMap { byteGraph =>
        getStream(
          byteGraph.body.namespace.as[String](),
          byteGraph.body.name.as[String](),
          byteGraph.body.withPlacement.as[Boolean]()
        ).map { stream =>
          Protocol.httpResponse(
            body = Protocol.MethodGetStream.response(
              requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
              stream = stream.map(_.row(byteGraph.body.namespace.as[String]())),
              writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
            ),
            contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
            renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
          )
        }
      }
  }

  private def getStream(namespace: String, name: String, withPlacement: Boolean): Future[Option[NamespaceHelper.Stream]] = {
    val stream = namespaceHelper.getStream(namespace, name)
    if (withPlacement) resolvePlacement(namespace, stream)
    else stream
  }

  private def resolvePlacement(namespace: String, stream: Future[Option[NamespaceHelper.Stream]]): Future[Option[NamespaceHelper.Stream]] = {
    stream.flatMap {
      case None => Future.None
      case Some(stream) => namespaceHelper.placement(namespace, stream).map(Some(_))
    }
  }

}
