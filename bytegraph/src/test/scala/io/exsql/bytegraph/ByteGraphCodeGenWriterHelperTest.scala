package io.exsql.bytegraph

import java.time.{LocalDate, ZonedDateTime}
import java.util

import ByteGraph.{ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphListType, ByteGraphSchemaField}
import io.exsql.bytegraph.metadata.ByteGraphSchema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ByteGraphCodeGenWriterHelperTest extends AnyWordSpec with Matchers {

  private val byteGraphSchema = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("boolean", ByteGraphValueType.Boolean),
    ByteGraphSchemaField("aBoolean", ByteGraphValueType.Boolean),
    ByteGraphSchemaField("aShort", ByteGraphValueType.Short),
    ByteGraphSchemaField("anInt", ByteGraphValueType.Int),
    ByteGraphSchemaField("aLong", ByteGraphValueType.Long),
    ByteGraphSchemaField("aFloat", ByteGraphValueType.Float),
    ByteGraphSchemaField("aDouble", ByteGraphValueType.Double),
    ByteGraphSchemaField("string", ByteGraphValueType.String),
    ByteGraphSchemaField("localDate", ByteGraphValueType.Date),
    ByteGraphSchemaField("zonedDateTime", ByteGraphValueType.DateTime),
    ByteGraphSchemaField("bytes", ByteGraphValueType.Blob),
    ByteGraphSchemaField("strings", ByteGraphListType(ByteGraphValueType.String)),
    ByteGraphSchemaField("nested", ByteGraphSchema.rowOf(
      ByteGraphSchemaField("n_boolean", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("n_string", ByteGraphValueType.String),
      ByteGraphSchemaField("n_strings", ByteGraphListType(ByteGraphValueType.String))
    )),
    ByteGraphSchemaField("dynamic", ByteGraphValueType.Structure),
    ByteGraphSchemaField("reference", ByteGraphSchema.referenceOf("nested")),
    ByteGraphSchemaField("listOfNested", ByteGraphListType(
      ByteGraphSchema.rowOf(
        ByteGraphSchemaField("ln_boolean", ByteGraphValueType.Boolean),
        ByteGraphSchemaField("ln_string", ByteGraphValueType.String),
        ByteGraphSchemaField("ln_strings", ByteGraphListType(ByteGraphValueType.String))
      )
    ))
  )

  "A ByteGraphCodeGenWriterHelper" when {
    "using a writer generated for an object array" should {
      "succeed" in {
        val buffer = Utf8Utils.encode("abcdefgh")
        val row: Array[AnyRef] = Array[AnyRef](
          true.asInstanceOf[AnyRef],
          true.asInstanceOf[AnyRef],
          2.toShort.asInstanceOf[AnyRef],
          3.asInstanceOf[AnyRef],
          50L.asInstanceOf[AnyRef],
          2.5f.asInstanceOf[AnyRef],
          1.5d.asInstanceOf[AnyRef],
          "string",
          LocalDate.of(2018,1,1),
          ZonedDateTime.parse("2018-01-01T00:00:00Z"),
          buffer,
          Array("string1", "string2"),
          Array[AnyRef](
            true.asInstanceOf[AnyRef],
            "string3",
            Array("string4", "string5")
          ),
          ByteGraph
            .structureBuilder()
            .appendString("key", "value")
            .appendBoolean("success", value = true)
            .build(),
          Array[AnyRef](
            Array[AnyRef](true.asInstanceOf[AnyRef], "string6", Array("string7", "string8")),
            Array[AnyRef](false.asInstanceOf[AnyRef], "string9", Array("string10"))
          )
        )

        val writer = ByteGraph.writer(byteGraphSchema)
        val bytes = writer.write(row, true)

        val reader = ByteGraph.reader(writer.schema)
        val sqlResponse = reader.read(bytes.bytes())
        sqlResponse(0) should be (java.lang.Boolean.TRUE)
        sqlResponse(1) should be (java.lang.Boolean.TRUE)
        sqlResponse(2) should be (java.lang.Short.valueOf(2.toShort))
        sqlResponse(3) should be (java.lang.Integer.valueOf(3))
        sqlResponse(4) should be (java.lang.Long.valueOf(50L))
        sqlResponse(5) should be (java.lang.Float.valueOf(2.5f))
        sqlResponse(6) should be (java.lang.Double.valueOf(1.5d))
        sqlResponse(7) should be ("string")
        sqlResponse(8) should be (LocalDate.of(2018,1,1))
        sqlResponse(9) should be (ZonedDateTime.parse("2018-01-01T00:00:00Z"))
        Utf8Utils.decode(sqlResponse(10).asInstanceOf[Array[Byte]]) should be ("abcdefgh")
        util.Arrays.deepEquals(sqlResponse(11).asInstanceOf[Array[AnyRef]], Array("string1", "string2")) should be (true)

        val nested: Array[AnyRef] = sqlResponse(12).asInstanceOf[Array[AnyRef]]
        nested(0) should be (java.lang.Boolean.TRUE)
        nested(1) should be ("string3")
        util.Arrays.deepEquals(nested(2).asInstanceOf[Array[AnyRef]], Array("string4", "string5")) should be (true)

        val dynamic: ByteGraphStructure = sqlResponse(13).asInstanceOf[ByteGraphStructure]
        dynamic("key").get.as[String]() should be ("value")
        dynamic("success").get.as[Boolean]() should be (true)

        val reference: Array[AnyRef] = sqlResponse(14).asInstanceOf[Array[AnyRef]]
        reference(0) should be (java.lang.Boolean.TRUE)
        reference(1) should be ("string3")
        util.Arrays.deepEquals(reference(2).asInstanceOf[Array[AnyRef]], Array("string4", "string5")) should be (true)

        val listOfNested = sqlResponse(15).asInstanceOf[Array[AnyRef]]
        val nested0 = listOfNested(0).asInstanceOf[Array[AnyRef]]
        nested0(0) should be (java.lang.Boolean.TRUE)
        nested0(1) should be ("string6")
        util.Arrays.deepEquals(nested0(2).asInstanceOf[Array[AnyRef]], Array("string7", "string8")) should be (true)

        val nested1 = listOfNested(1).asInstanceOf[Array[AnyRef]]
        nested1(0) should be (java.lang.Boolean.FALSE)
        nested1(1) should be ("string9")
        util.Arrays.deepEquals(nested1(2).asInstanceOf[Array[AnyRef]], Array("string10")) should be (true)
      }
    }

    "supports empty array" in {
      val writer = ByteGraph.writer(byteGraphSchema)
      val bytes = writer.write(Array.empty, true)

      ByteGraph.toJsonString(ByteGraph.valueOf(bytes, Some(byteGraphSchema)).as[ByteGraphRow]()) should be (
        "[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]"
      )
    }
  }

}
