package io.exsql.bytegraph

import java.time.{LocalDate, ZonedDateTime}

import ByteGraph.{ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchemaField
import io.exsql.bytegraph.metadata.ByteGraphSchema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ByteGraphJsonHelperTest extends AnyWordSpec with Matchers {

  "A ByteGraphJsonHelper" when {
    "parsing a json string" should {
      "succeed" in {
        val json =
          s"""
            |{
            |  "nullable": null,
            |  "array": [1,2,3],
            |  "struct": {
            |     "1": 2,
            |     "3": 4
            |  },
            |  "boolean": true,
            |  "int": 1,
            |  "long": ${Long.MinValue},
            |  "double": 1.5,
            |  "string": "text",
            |  "mixin": [1, {"2":3, "4": [true,false]}]
            |}
          """.stripMargin

        ByteGraph.toJsonString(ByteGraph.fromJson(json)) should be (
          s"""{"nullable":null,"array":[1,2,3],"struct":{"1":2,"3":4},"boolean":true,"int":1,"long":${Long.MinValue},"double":1.5,"string":"text","mixin":[1,{"2":3,"4":[true,false]}]}"""
        )
      }

      "support the parsing a json with a schema" in {
        val json =
          s"""
             |{
             |  "entry": {
             |    "key": "key",
             |    "value": {
             |      "dateTime": "2019-01-01T00:00:00Z",
             |      "date": "2019-01-01",
             |      "string": "string"
             |    }
             |  }
             |}
          """.stripMargin

        val jsonSchema =
          """
            |{
            |  "type":"structure",
            |  "fields":[{"name":"entry","type":"structure","fields":[
            |    {"name":"value","type":"structure","fields":[{"name":"dateTime","type":"datetime"},{"name":"date","type":"date"}]}
            |  ]}]
            |}
          """.stripMargin

        val byteGraph = ByteGraph.fromJson(json, Some(ByteGraphSchema.fromJson(jsonSchema))).as[ByteGraphStructure]()
        val entry = byteGraph("entry").get.as[ByteGraphStructure]()
        entry("key").get.as[String]() should be ("key")

        val value = entry("value").get.as[ByteGraphStructure]()
        value("dateTime").get.as[ZonedDateTime]() should be (ZonedDateTime.parse("2019-01-01T00:00:00Z"))
        value("date").get.as[LocalDate]() should be (LocalDate.parse("2019-01-01"))
        value("string").get.as[String]() should be ("string")
      }

      "support a list of rows in row schema mode" in {
        val json =
          s"""
             |[
             |  [
             |    [
             |      "key",
             |      {
             |        "dateTime":"2019-01-01T00:00:00Z",
             |        "date":"2019-01-01",
             |        "string":"string"
             |      }
             |    ],
             |    [
             |      "key2",
             |      {
             |        "dateTime":"2019-01-02T00:00:00Z",
             |        "date":"2019-01-02",
             |        "string":"string2"
             |      }
             |    ]
             |  ]
             |]
          """.stripMargin

        val jsonSchema =
          """
            |{
            |  "type":"row",
            |  "fields":[{"name":"entries","type":"list","elements":{"type":"row","fields":[
            |    {"name":"key","type":"string"},
            |    {"name":"value","type":"structure","fields":[{"name":"dateTime","type":"datetime"},{"name":"date","type":"date"}]}
            |  ]}}]
            |}
          """.stripMargin

        val byteGraphSchema = ByteGraphSchema.fromJson(jsonSchema)
        val byteGraph = ByteGraph.fromJson(json, Some(byteGraphSchema)).as[ByteGraphRow]()
        val entries = byteGraph("entries").get.as[ByteGraphList]()

        val iterator = entries.iterator
        var entry = iterator.next().as[ByteGraphRow]()
        entry("key").get.as[String]() should be ("key")

        var value = entry("value").get.as[ByteGraphStructure]()
        value("dateTime").get.as[ZonedDateTime]() should be (ZonedDateTime.parse("2019-01-01T00:00:00Z"))
        value("date").get.as[LocalDate]() should be (LocalDate.parse("2019-01-01"))
        value("string").get.as[String]() should be ("string")

        entry = iterator.next().as[ByteGraphRow]()
        entry("key").get.as[String]() should be ("key2")

        value = entry("value").get.as[ByteGraphStructure]()
        value("dateTime").get.as[ZonedDateTime]() should be (ZonedDateTime.parse("2019-01-02T00:00:00Z"))
        value("date").get.as[LocalDate]() should be (LocalDate.parse("2019-01-02"))
        value("string").get.as[String]() should be ("string2")
      }

      "support converting a json document to a row" in {
        val schema = ByteGraphSchema.rowOf(
          ByteGraphSchemaField("nullable", ByteGraphValueType.Null),
          ByteGraphSchemaField("array", ByteGraphSchema.listOf(ByteGraphValueType.Int)),
          ByteGraphSchemaField("struct", ByteGraphSchema.structureOf(
            ByteGraphSchemaField("1", ByteGraphValueType.Int),
            ByteGraphSchemaField("3", ByteGraphValueType.Int)
          )),
          ByteGraphSchemaField("boolean", ByteGraphValueType.Boolean),
          ByteGraphSchemaField("int", ByteGraphValueType.Int),
          ByteGraphSchemaField("long", ByteGraphValueType.Long),
          ByteGraphSchemaField("double", ByteGraphValueType.Double),
          ByteGraphSchemaField("string", ByteGraphValueType.String),
          ByteGraphSchemaField("mixin", ByteGraphValueType.List)
        )

        val json =
          s"""
             |{
             |  "nullable": null,
             |  "array": [1,2,3],
             |  "struct": {
             |     "1": 2,
             |     "3": 4
             |  },
             |  "boolean": true,
             |  "int": 1,
             |  "long": ${Long.MinValue},
             |  "double": 1.5,
             |  "string": "text",
             |  "mixin": [1, {"2":3, "4": [true,false]}]
             |}
          """.stripMargin

        ByteGraph.toJsonString(ByteGraph.fromJson(json, Some(schema))) should be (
          s"""[null,[1,2,3],{"1":2,"3":4},true,1,-9223372036854775808,1.5,"text",[1,{"2":3,"4":[true,false]}]]"""
        )
      }
    }

    "converting a binary value to json" should {
      "succeed" in {
        val byteGraphBuilder = ByteGraph.structureBuilder()
        byteGraphBuilder.appendNull("nullable")
        byteGraphBuilder.appendList("array")(
          _.appendInt(1).appendInt(2).appendInt(3)
        )

        byteGraphBuilder.appendStructure("struct")(
          _.appendInt("1", 2).appendInt("3", 4)
        )

        byteGraphBuilder.appendBoolean("boolean", value = true)
        byteGraphBuilder.appendInt("int", 1)
        byteGraphBuilder.appendLong("long", Long.MinValue)
        byteGraphBuilder.appendDouble("double", 1.5)
        byteGraphBuilder.appendString("string", "text")
        byteGraphBuilder.appendList("mixin") { listBuilder =>
          listBuilder
            .appendInt(1)
            .appendStructure { structureBuilder =>
              structureBuilder
                .appendInt("2", 3)
                .appendList("4")(_.appendBoolean(true).appendBoolean(false))
            }
        }

        val json = ByteGraph.toJsonString(byteGraphBuilder.build())
        json should be (
          """{"nullable":null,"array":[1,2,3],"struct":{"1":2,"3":4},"boolean":true,"int":1,"long":-9223372036854775808,"double":1.5,"string":"text","mixin":[1,{"2":3,"4":[true,false]}]}"""
        )
      }

      "manage date time properly even when partial" in {
        val byteGraphBuilder = ByteGraph.structureBuilder()
        byteGraphBuilder.appendDateTime("dateTime", ZonedDateTime.parse("2010-01-01T00:00Z"))

        val json = ByteGraph.toJsonString(byteGraphBuilder.build())
        json should be ("""{"dateTime":"2010-01-01T00:00:00Z"}""")
      }

      "manage date properly" in {
        val byteGraphBuilder = ByteGraph.structureBuilder()
        byteGraphBuilder.appendDate("date", LocalDate.parse("2010-01-01"))

        val json = ByteGraph.toJsonString(byteGraphBuilder.build())
        json should be ("""{"date":"2010-01-01"}""")
      }
    }
  }

}
