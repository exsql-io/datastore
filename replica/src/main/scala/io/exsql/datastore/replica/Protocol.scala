package io.exsql.datastore.replica

import java.time.{Duration, LocalDate, ZonedDateTime}
import java.util.Base64
import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.bytes.ByteArrayByteGraphBytes
import io.exsql.bytegraph.ipc.ByteGraphMessageBuilder
import io.exsql.bytegraph.metadata.ByteGraphSchema
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphRowType, ByteGraphSchema, ByteGraphSchemaField, ByteGraphStructureType}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import com.twitter.finagle.http
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, StorageUnit}
import okhttp3._

import scala.collection.compat.immutable.ArraySeq

object Protocol {

  val BinaryContentType: String = "application/bytegraph"

  val BinaryMediaType: MediaType = MediaType.get(BinaryContentType)

  val TextContentType: String = "application/json"

  val TopicPrefix: String = "reactive-database"

  val DefaultMaxMessageSize: StorageUnit = StorageUnit.fromMegabytes(1024)

  val StreamDefinition: ByteGraphRowType = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("namespace", ByteGraphValueType.String),
    ByteGraphSchemaField("name", ByteGraphValueType.String),
    ByteGraphSchemaField("schema", ByteGraphValueType.Structure),
    ByteGraphSchemaField("partitions", ByteGraphValueType.Int),
    ByteGraphSchemaField("writeReplicas", ByteGraphValueType.Short),
    ByteGraphSchemaField("deployReplicas", ByteGraphValueType.Short),
    ByteGraphSchemaField("placement", ByteGraphValueType.Structure)
  )

  val SqlViewDefinition: ByteGraphRowType = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("namespace", ByteGraphValueType.String),
    ByteGraphSchemaField("name", ByteGraphValueType.String),
    ByteGraphSchemaField("sql", ByteGraphValueType.String),
    ByteGraphSchemaField("schema", ByteGraphValueType.Structure)
  )

  val NamespaceDefinition: ByteGraphRowType = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("name", ByteGraphValueType.String),
    ByteGraphSchemaField("replicas", ByteGraphValueType.Short),
    ByteGraphSchemaField("streams", ByteGraphSchema.listOf(StreamDefinition)),
    ByteGraphSchemaField("sqlViews", ByteGraphSchema.listOf(SqlViewDefinition))
  )

  val StreamPartitionStatisticsDefinition: ByteGraphRowType = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("namespace", ByteGraphValueType.String),
    ByteGraphSchemaField("name", ByteGraphValueType.String),
    ByteGraphSchemaField("partition", ByteGraphValueType.Int),
    ByteGraphSchemaField("statistics", ByteGraphSchema.rowOf(
      ByteGraphSchemaField("rows", ByteGraphValueType.Long),
      ByteGraphSchemaField("bytes", ByteGraphValueType.Long)
    ))
  )

  private val client = new OkHttpClient()

  private val serviceName: String = "io.exsql.datastore.replica.ReactiveDatabaseReplicaService"

  private val controllerServiceName: String = "io.exsql.datastore.controller.ReactiveDatabaseControllerService"

  private val requestIdHeaderDefinition = ByteGraphSchemaField("requestId", ByteGraphValueType.String)

  private val showSchemaHeaderDefinition = ByteGraphSchemaField("showSchema", ByteGraphValueType.Boolean)

  private val renderAsDocumentHeaderDefinition = ByteGraphSchemaField("renderAsDocument", ByteGraphValueType.Boolean)

  private val allowResponseMessageBuilder = ByteGraphMessageBuilder.forRow(
    headersDefinition = ByteGraphSchema.rowOf(
      requestIdHeaderDefinition, showSchemaHeaderDefinition, renderAsDocumentHeaderDefinition
    ),
    bodyDefinition = ByteGraphSchema.rowOf()
  )

  object MethodStatus {

    val Name: String = s"/$serviceName/Status"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf()
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("serverInfo", ByteGraphSchema.rowOf(
          ByteGraphSchemaField("loadAverage", ByteGraphValueType.Double),
          ByteGraphSchemaField("memoryInfo", ByteGraphSchema.rowOf(
            ByteGraphSchemaField("used", ByteGraphValueType.Long),
            ByteGraphSchemaField("free", ByteGraphValueType.Long),
            ByteGraphSchemaField("max", ByteGraphValueType.Long)
          )),
          ByteGraphSchemaField("buildInfo", ByteGraphSchema.rowOf(
            ByteGraphSchemaField("version", ByteGraphValueType.String)
          ))
        ))
      )
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), None, writeSchema)
    }

    def response(requestId: String, serverInfo: Array[AnyRef], writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(serverInfo), writeSchema)
    }

  }

  object MethodGetStream {

    val Name: String = s"$controllerServiceName/GetStream"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("name", ByteGraphValueType.String),
        ByteGraphSchemaField("withPlacement", ByteGraphValueType.Boolean)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = StreamDefinition
    )

    def execute(url: String,
                requestId: String,
                namespace: String,
                name: String,
                withPlacement: Boolean = false): ByteGraph = {

      val body = RequestBody.create(
        toByteArray(requestMessageBuilder.build(
          Array(requestId), Some(Array[AnyRef](namespace, name, java.lang.Boolean.valueOf(withPlacement)))
        )),
        BinaryMediaType
      )

      val httpRequest = new Request.Builder().url(s"$url/$Name").post(body).build()
      toByteGraph(client.newCall(httpRequest).execute(), responseMessageBuilder.schema)
    }

  }

  object MethodGet {

    val Name: String = s"/$serviceName/Get"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("stream", ByteGraphValueType.String),
        ByteGraphSchemaField("key", ByteGraphValueType.String)
      )
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def request(requestId: String,
                namespace: String,
                stream: String,
                key: String,
                writeSchema: Boolean): ByteGraph = {

      requestMessageBuilder.build(
        Array(requestId),
        Some(Array(namespace, stream, key)),
        writeSchema
      )
    }

    def response(requestId: String,
                 value: Option[Array[AnyRef]],
                 valueSchema: ByteGraphSchema,
                 writeSchema: Boolean): ByteGraph = {

      val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
        headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
        bodyDefinition = valueSchema.asInstanceOf[ByteGraphRowType]
      )

      responseMessageBuilder.build(Array(requestId), value, writeSchema)
    }

  }

  object MethodGetAll {

    val Name: String = s"/$serviceName/GetAll"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("stream", ByteGraphValueType.String),
        ByteGraphSchemaField("keys", ByteGraphSchema.listOf(ByteGraphValueType.String))
      )
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def request(requestId: String,
                namespace: String,
                stream: String,
                keys: Set[String],
                writeSchema: Boolean): ByteGraph = {

      requestMessageBuilder.build(
        Array(requestId),
        Some(Array(namespace, stream, keys.toArray)),
        writeSchema
      )
    }

    def response(requestId: String,
                 values: Iterator[Array[AnyRef]],
                 valueSchema: ByteGraphSchema,
                 writeSchema: Boolean): ByteGraph = {

      val responseMessageBuilder = ByteGraphMessageBuilder.forRows(
        headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
        bodyDefinition = valueSchema.asInstanceOf[ByteGraphRowType]
      )

      responseMessageBuilder.build(Array(requestId), values, writeSchema)
    }

  }

  object MethodRead {

    val Name: String = s"/$serviceName/Read"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition, showSchemaHeaderDefinition, renderAsDocumentHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("stream", ByteGraphValueType.String),
        ByteGraphSchemaField("schema", ByteGraphValueType.Structure)
      )
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def request(requestId: String,
                namespace: String,
                stream: String,
                schema: ByteGraphStructure,
                writeSchema: Boolean): ByteGraph = {

      requestMessageBuilder.build(
        Array(requestId),
        Some(Array(namespace, stream, schema)),
        writeSchema
      )
    }

    def response(requestId: String,
                 values: Iterator[Array[AnyRef]],
                 valueSchema: ByteGraphSchema,
                 writeSchema: Boolean): ByteGraph = {

      val responseMessageBuilder = ByteGraphMessageBuilder.forRows(
        headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
        bodyDefinition = valueSchema.asInstanceOf[ByteGraphRowType]
      )

      responseMessageBuilder.build(Array(requestId), values, writeSchema)
    }

  }

  object MethodListNamespaces {

    val Name: String = s"$controllerServiceName/ListNamespaces"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("withPlacement", ByteGraphValueType.Boolean))
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRows(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = NamespaceDefinition
    )

    def execute(url: String, requestId: String): ByteGraph = {
      val body = RequestBody.create(
        toByteArray(requestMessageBuilder.build(Array(requestId), Some(Array[AnyRef](java.lang.Boolean.TRUE)))),
        BinaryMediaType
      )

      val httpRequest = new Request.Builder().url(s"$url/$Name").post(body).build()
      toByteGraph(client.newCall(httpRequest).execute(), responseMessageBuilder.schema)
    }

  }

  object MethodGetStreamPartitionStatistics {

    val Name: String = s"/$serviceName/GetStreamPartitionStatistics"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("stream", ByteGraphValueType.String),
        ByteGraphSchemaField("partition", ByteGraphValueType.Int)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = StreamPartitionStatisticsDefinition
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String,
                namespace: String,
                stream: String,
                partition: Int,
                writeSchema: Boolean): ByteGraph = {

      requestMessageBuilder.build(
        Array(requestId),
        Some(Array(namespace, stream, Integer.valueOf(partition))),
        writeSchema
      )
    }

    def response(requestId: String,
                 namespace: String,
                 stream: String,
                 partition: Int,
                 statistics: Array[AnyRef],
                 writeSchema: Boolean): ByteGraph = {

      responseMessageBuilder.build(
        Array(requestId),
        Some(Array[AnyRef](namespace, stream, Integer.valueOf(partition), statistics)),
        writeSchema
      )
    }

    def execute(url: String,
                requestId: String,
                namespace: String,
                stream: String,
                partition: Int): ByteGraph = {

      val body = RequestBody.create(
        toByteArray(requestMessageBuilder.build(
          Array(requestId), Some(Array[AnyRef](namespace, stream, Integer.valueOf(partition)))
        )),
        BinaryMediaType
      )

      val httpRequest = new Request.Builder().url(s"$url$Name").post(body).build()
      toByteGraph(client.newCall(httpRequest).execute(), responseMessageBuilder.schema)
    }

  }

  object MethodCreateSqlView {

    val Name: String = s"$controllerServiceName/CreateSqlView"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("name", ByteGraphValueType.String),
        ByteGraphSchemaField("sql", ByteGraphValueType.String),
        ByteGraphSchemaField("schema", ByteGraphValueType.Structure)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("success", ByteGraphValueType.Boolean))
    )

    def execute(url: String,
                requestId: String,
                namespace: String,
                name: String,
                sql: String,
                schema: ByteGraphStructure): ByteGraph = {

      val body = RequestBody.create(
        toByteArray(requestMessageBuilder.build(
          Array(requestId), Some(Array[AnyRef](namespace, name, sql, schema))
        )),
        BinaryMediaType
      )

      val httpRequest = new Request.Builder().url(s"$url/$Name").post(body).build()
      toByteGraph(client.newCall(httpRequest).execute(), responseMessageBuilder.schema)
    }

  }

  object MethodDropSqlView {

    val Name: String = s"$controllerServiceName/DropSqlView"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("name", ByteGraphValueType.String)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("success", ByteGraphValueType.Boolean))
    )

    def execute(url: String, requestId: String, namespace: String, name: String): ByteGraph = {
      val body = RequestBody.create(
        toByteArray(requestMessageBuilder.build(
          Array(requestId), Some(Array[AnyRef](namespace, name))
        )),
        BinaryMediaType
      )

      val httpRequest = new Request.Builder().url(s"$url/$Name").post(body).build()
      toByteGraph(client.newCall(httpRequest).execute(), responseMessageBuilder.schema)
    }

  }

  object MethodGetSqlView {

    val Name: String = s"$controllerServiceName/GetSqlView"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("name", ByteGraphValueType.String)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = SqlViewDefinition
    )

    def execute(url: String,
                requestId: String,
                namespace: String,
                name: String): ByteGraph = {

      val body = RequestBody.create(
        toByteArray(requestMessageBuilder.build(
          Array(requestId), Some(Array[AnyRef](namespace, name))
        )),
        BinaryMediaType
      )

      val httpRequest = new Request.Builder().url(s"$url/$Name").post(body).build()
      toByteGraph(client.newCall(httpRequest).execute(), responseMessageBuilder.schema)
    }

  }

  def allowResponse(request: http.Request, schema: ByteGraphSchema): Future[http.Response] = {
    fromRequest(request, schema).map { byteGraph =>
      val requestId = byteGraph.requestId.option[String]()
      val contentType: String = request.contentType.getOrElse(BinaryContentType)
      allowResponse(requestId, contentType)
    }
  }

  private def allowResponse(requestId: Option[String], contentType: String): http.Response = {
    val response = httpResponse(allowResponseMessageBuilder.build(Array(requestId.orNull), None), contentType)
    response
      .headerMap
      .add(com.twitter.finagle.http.Fields.AccessControlAllowMethods, s"${com.twitter.finagle.http.Method.Post.name}, ${com.twitter.finagle.http.Method.Options.name}")
      .add(
        com.twitter.finagle.http.Fields.AccessControlAllowHeaders,
        s"${com.twitter.finagle.http.Fields.ContentType}, ${com.twitter.finagle.http.Fields.Accept}, ${com.twitter.finagle.http.Fields.Origin}, ${com.twitter.finagle.http.Fields.AccessControlAllowOrigin}, X-Requested-With"
      )

    response
  }

  def httpResponse(body: ByteGraph, contentType: String, renderAsDocument: Boolean = false): http.Response = {
    val justContentType = contentType.replace(";charset=utf-8", "")

    val response = com.twitter.finagle.http.Response(com.twitter.finagle.http.Status.Ok)

    val content = toBuf(body, justContentType, renderAsDocument)
    if (!content.isEmpty) response.content(content)
    response.setContentType(justContentType)
    response.headerMap.add(com.twitter.finagle.http.Fields.AccessControlAllowOrigin, "*")

    response
  }

  def fromRequest(request: http.Request, schema: ByteGraphSchema): Future[ByteGraph] = {
    request.method match {
      case http.Method.Get => fromGetRequest(request, schema)
      case _ =>
        val stream = Reader.toAsyncStream(request.reader)
        stream.toSeq().map(Buf(_)).map { buf =>
          request.contentType match {
            case Some(contentType) if contentType.contains(TextContentType) =>
              ByteGraph.fromJson(
                new String(fromBuf(buf), "UTF-8"),
                Some(convertToByteGraphStructureType(schema))
              ).as[ByteGraphStructure]()

            case _ => ByteGraph.valueOf(ByteArrayByteGraphBytes(fromBuf(buf)), Some(schema)).as[ByteGraphRow]()
          }
        }
    }
  }

  private def fromBuf(buf: Buf): Array[Byte] = {
    Buf.ByteArray.Owned.extract(buf)
  }

  private def toBuf(byteGraph: ByteGraph, contentType: String, renderAsDocument: Boolean): Buf = {
    val bytes: Array[Byte] = {
      if (contentType.startsWith(TextContentType)) ByteGraph.toJson(byteGraph, renderAsDocument)
      else ByteGraph.render(byteGraph, withVersionFlag = true).bytes()
    }

    Buf.ByteArray.Owned(bytes)
  }

  private def toByteArray(byteGraph: ByteGraph): Array[Byte] = {
    ByteGraph.render(byteGraph, withVersionFlag = true).bytes()
  }

  private def toByteGraph(response: Response, schema: ByteGraphSchema): ByteGraph = {
    ByteGraph.valueOf(ByteArrayByteGraphBytes(response.body().bytes()), Some(schema))
  }

  private def fromGetRequest(request: http.Request, schema: ByteGraphSchema): Future[ByteGraph] = Future {
    val builder = ByteGraph.rowBuilder(schema.asInstanceOf[ByteGraphRowType])
    schema.fields.foreach { field =>
      if (!request.containsParam(field.name)) builder.appendNull()
      else {
        (field.byteGraphDataType.valueType: @unchecked) match {
          case ByteGraphValueType.Null => builder.appendNull()
          case ByteGraphValueType.Boolean => builder.appendBoolean(request.getBooleanParam(field.name))
          case ByteGraphValueType.Short => builder.appendShort(request.getShortParam(field.name))
          case ByteGraphValueType.Int => builder.appendInt(request.getIntParam(field.name))
          case ByteGraphValueType.Long => builder.appendLong(request.getLongParam(field.name))
          case ByteGraphValueType.UShort => builder.appendUShort(request.getShortParam(field.name))
          case ByteGraphValueType.UInt => builder.appendUInt(request.getIntParam(field.name))
          case ByteGraphValueType.ULong => builder.appendULong(request.getLongParam(field.name))
          case ByteGraphValueType.Float => builder.appendFloat(request.getParam(field.name).toFloat)
          case ByteGraphValueType.Double => builder.appendDouble(request.getParam(field.name).toDouble)
          case ByteGraphValueType.Date => builder.appendDate(LocalDate.parse(request.getParam(field.name)))
          case ByteGraphValueType.DateTime => builder.appendDateTime(ZonedDateTime.parse(request.getParam(field.name)))
          case ByteGraphValueType.Duration => builder.appendDuration(Duration.parse(request.getParam(field.name)))
          case ByteGraphValueType.String => builder.appendString(request.getParam(field.name))
          case ByteGraphValueType.Blob => builder.appendBlob(Base64.getDecoder.decode(request.getParam(field.name)))
          case ByteGraphValueType.List => ByteGraph.fromJson(request.getParam(field.name)).as[ByteGraphList]()
          case ByteGraphValueType.Structure =>
            ByteGraph
              .fromJson(request.getParam(field.name), Some(field.byteGraphDataType.asInstanceOf[ByteGraphStructureType]))
              .as[ByteGraphStructure]()

          case ByteGraphValueType.Row =>
            ByteGraph
              .fromJson(request.getParam(field.name), Some(field.byteGraphDataType.asInstanceOf[ByteGraphRowType]))
              .as[ByteGraphRowType]()

          case ByteGraphValueType.Symbol => ???
          case ByteGraphValueType.Reference => ???
        }
      }
    }

    builder.build()
  }

  private def convertToByteGraphStructureType(byteGraphSchema: ByteGraphSchema): ByteGraphStructureType = {
    if (byteGraphSchema.isInstanceOf[ByteGraphStructureType]) byteGraphSchema.asInstanceOf[ByteGraphStructureType]
    else {
      ByteGraphSchema.structureOf(ArraySeq.unsafeWrapArray(byteGraphSchema.fields).map { field =>
        field.byteGraphDataType match {
          case byteGraphRowType: ByteGraphRowType =>
            field.copy(byteGraphDataType = convertToByteGraphStructureType(byteGraphRowType))

          case _ => field
        }
      }: _*)
    }
  }

}
