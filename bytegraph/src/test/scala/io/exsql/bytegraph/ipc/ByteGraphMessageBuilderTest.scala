package io.exsql.bytegraph.ipc

import io.exsql.bytegraph.ByteGraph.{ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchemaField
import io.exsql.bytegraph.metadata.ByteGraphSchema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ByteGraphMessageBuilderTest extends AnyWordSpec with Matchers {

  private val headerDefinition = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("requestId", ByteGraphValueType.String),
    ByteGraphSchemaField("flag", ByteGraphValueType.Boolean)
  )

  private val simpleBodyDefinition = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("success", ByteGraphValueType.Boolean),
    ByteGraphSchemaField("values", ByteGraphSchema.rowOf(
      ByteGraphSchemaField("flag", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("value", ByteGraphValueType.String)
    ))
  )

  "A ByteGraphMessageBuilder" when {
    "creating a simple message" should {
      "succeed" in {
        val message = ByteGraphMessageBuilder
          .forRow(headerDefinition, simpleBodyDefinition)
          .build(
            Array[AnyRef]("request-id", java.lang.Boolean.FALSE),
            Some(Array[AnyRef](java.lang.Boolean.TRUE, Array[AnyRef](java.lang.Boolean.TRUE, "content")))
          )

        ByteGraph.toJsonString(message.selectDynamic("schema").as[ByteGraphStructure]()) should be (
          """{"type":"row","fields":[{"name":"schema","type":"structure","metadata":{}},{"name":"headers","type":"row","fields":[{"name":"requestId","type":"string","metadata":{}},{"name":"flag","type":"boolean","metadata":{}}],"metadata":{}},{"name":"body","type":"row","fields":[{"name":"success","type":"boolean","metadata":{}},{"name":"values","type":"row","fields":[{"name":"flag","type":"boolean","metadata":{}},{"name":"value","type":"string","metadata":{}}],"metadata":{}}],"metadata":{}}]}"""
        )

        message.headers.requestId.as[String]() should be ("request-id")
        message.headers.flag.as[Boolean]() should be (false)
        message.body.success.as[Boolean]() should be (true)
        message.body.values.flag.as[Boolean]() should be (true)
        message.body.values.value.as[String]() should be ("content")
      }

      "support building a message with empty body" in {
        val message = ByteGraphMessageBuilder
          .forRow(headerDefinition, simpleBodyDefinition)
          .build(
            Array[AnyRef]("request-id", java.lang.Boolean.FALSE),
            None
          )

        ByteGraph.toJsonString(message.selectDynamic("schema").as[ByteGraphStructure]()) should be (
          """{"type":"row","fields":[{"name":"schema","type":"structure","metadata":{}},{"name":"headers","type":"row","fields":[{"name":"requestId","type":"string","metadata":{}},{"name":"flag","type":"boolean","metadata":{}}],"metadata":{}},{"name":"body","type":"row","fields":[{"name":"success","type":"boolean","metadata":{}},{"name":"values","type":"row","fields":[{"name":"flag","type":"boolean","metadata":{}},{"name":"value","type":"string","metadata":{}}],"metadata":{}}],"metadata":{}}]}"""
        )

        message.headers.requestId.as[String]() should be ("request-id")
        message.headers.flag.as[Boolean]() should be (false)
        message.body.success.option[Boolean]() should be (None)
        message.body.values.option[ByteGraphRow]() should be (None)
      }

      "support building a message with empty headers and body" in {
        val message = ByteGraphMessageBuilder
          .forRow(headerDefinition, simpleBodyDefinition)
          .build(Array.empty[AnyRef], None)

        ByteGraph.toJsonString(message.selectDynamic("schema").as[ByteGraphStructure]()) should be (
          """{"type":"row","fields":[{"name":"schema","type":"structure","metadata":{}},{"name":"headers","type":"row","fields":[{"name":"requestId","type":"string","metadata":{}},{"name":"flag","type":"boolean","metadata":{}}],"metadata":{}},{"name":"body","type":"row","fields":[{"name":"success","type":"boolean","metadata":{}},{"name":"values","type":"row","fields":[{"name":"flag","type":"boolean","metadata":{}},{"name":"value","type":"string","metadata":{}}],"metadata":{}}],"metadata":{}}]}"""
        )

        message.headers.requestId.option[String]() should be (None)
        message.headers.flag.option[Boolean]() should be (None)
        message.body.success.option[Boolean]() should be (None)
        message.body.values.option[ByteGraphRow]() should be (None)
      }

      "support building a message without rendering the schema" in {
        val message = ByteGraphMessageBuilder
          .forRow(headerDefinition, simpleBodyDefinition)
          .build(
            Array[AnyRef]("request-id", java.lang.Boolean.FALSE),
            Some(Array[AnyRef](java.lang.Boolean.TRUE, Array[AnyRef](java.lang.Boolean.TRUE, "content"))),
            writeSchema = false
          )

        message.selectDynamic("schema").option[String]() should be (None)
        message.headers.requestId.as[String]() should be ("request-id")
        message.headers.flag.as[Boolean]() should be (false)
        message.body.success.as[Boolean]() should be (true)
        message.body.values.flag.as[Boolean]() should be (true)
        message.body.values.value.as[String]() should be ("content")
      }

      "support building a message with an existing content" in {
        val writer = ByteGraph.writer(simpleBodyDefinition)
        val byteGraphBytes = ByteGraph.render(
          writer.write(Array[AnyRef](java.lang.Boolean.TRUE, Array[AnyRef](java.lang.Boolean.TRUE, "content")), true),
          withVersionFlag = true
        )

        val message = ByteGraphMessageBuilder
          .forRow(headerDefinition, simpleBodyDefinition)
          .buildFromByteGraphBytes(
            Array[AnyRef]("request-id", java.lang.Boolean.FALSE),
            Some(byteGraphBytes.slice(5)),
            writeSchema = true
          )

        ByteGraph.toJsonString(message.selectDynamic("schema").as[ByteGraphStructure]()) should be (
          """{"type":"row","fields":[{"name":"schema","type":"structure","metadata":{}},{"name":"headers","type":"row","fields":[{"name":"requestId","type":"string","metadata":{}},{"name":"flag","type":"boolean","metadata":{}}],"metadata":{}},{"name":"body","type":"row","fields":[{"name":"success","type":"boolean","metadata":{}},{"name":"values","type":"row","fields":[{"name":"flag","type":"boolean","metadata":{}},{"name":"value","type":"string","metadata":{}}],"metadata":{}}],"metadata":{}}]}"""
        )

        message.headers.requestId.as[String]() should be ("request-id")
        message.headers.flag.as[Boolean]() should be (false)
        message.body.success.as[Boolean]() should be (true)
        message.body.values.flag.as[Boolean]() should be (true)
        message.body.values.value.as[String]() should be ("content")
      }
    }

    "creating a list message" should {
      "succeed" in {
        val message = ByteGraphMessageBuilder
          .forRows(headerDefinition, simpleBodyDefinition)
          .build(
            Array[AnyRef]("request-id", java.lang.Boolean.FALSE),
            Iterator(
              Array[AnyRef](java.lang.Boolean.TRUE, Array[AnyRef](java.lang.Boolean.TRUE, "content")),
              Array[AnyRef](java.lang.Boolean.FALSE, Array[AnyRef](java.lang.Boolean.FALSE, "content2"))
            )
          )

        ByteGraph.toJsonString(message.selectDynamic("schema").as[ByteGraphStructure]()) should be (
          """{"type":"row","fields":[{"name":"schema","type":"structure","metadata":{}},{"name":"headers","type":"row","fields":[{"name":"requestId","type":"string","metadata":{}},{"name":"flag","type":"boolean","metadata":{}}],"metadata":{}},{"name":"body","type":"list","elements":{"type":"row","fields":[{"name":"success","type":"boolean","metadata":{}},{"name":"values","type":"row","fields":[{"name":"flag","type":"boolean","metadata":{}},{"name":"value","type":"string","metadata":{}}],"metadata":{}}]},"metadata":{}}]}"""
        )

        message.headers.requestId.as[String]() should be("request-id")
        message.headers.flag.as[Boolean]() should be (false)

        val rows = message.body.as[ByteGraphList]().iterator
        var row = rows.next()
        row.success.as[Boolean]() should be (true)
        row.values.flag.as[Boolean]() should be (true)
        row.values.value.as[String]() should be ("content")

        row = rows.next()
        row.success.as[Boolean]() should be (false)
        row.values.flag.as[Boolean]() should be (false)
        row.values.value.as[String]() should be ("content2")
      }
    }

  }

}
