package io.exsql.bytegraph.schema

import io.exsql.bytegraph.ByteGraphValueType
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchemaField
import io.exsql.bytegraph.metadata._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class ByteGraphSchemaTest extends AnyFlatSpec with Matchers {

  "a ByteGraphSchema" should "support json serialization" in {
    val schema = ByteGraphSchema.rowOf(
      ByteGraphSchemaField("null", ByteGraphValueType.Null, mutable.Map("nullable" -> "true")),
      ByteGraphSchemaField("boolean", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("short", ByteGraphValueType.Short),
      ByteGraphSchemaField("int", ByteGraphValueType.Int),
      ByteGraphSchemaField("long", ByteGraphValueType.Long),
      ByteGraphSchemaField("ushort", ByteGraphValueType.UShort),
      ByteGraphSchemaField("uint", ByteGraphValueType.UInt),
      ByteGraphSchemaField("ulong", ByteGraphValueType.ULong),
      ByteGraphSchemaField("float", ByteGraphValueType.Float),
      ByteGraphSchemaField("double", ByteGraphValueType.Double),
      ByteGraphSchemaField("date", ByteGraphValueType.Date),
      ByteGraphSchemaField("datetime", ByteGraphValueType.DateTime),
      ByteGraphSchemaField("duration", ByteGraphValueType.Duration),
      ByteGraphSchemaField("reference", ByteGraphSchema.referenceOf("row")),
      ByteGraphSchemaField("string", ByteGraphValueType.String),
      ByteGraphSchemaField("blob", ByteGraphValueType.Blob),
      ByteGraphSchemaField("symbol", ByteGraphValueType.Symbol),
      ByteGraphSchemaField("structure", ByteGraphSchema.structureOf(
        ByteGraphSchemaField("string", ByteGraphValueType.String)
      )),
      ByteGraphSchemaField("list", ByteGraphSchema.listOf(ByteGraphValueType.String)),
      ByteGraphSchemaField("row", ByteGraphSchema.rowOf(
        ByteGraphSchemaField("string", ByteGraphValueType.String)
      ))
    ).json()

    schema should be (
      """{"type":"row","fields":[{"name":"null","type":"null","metadata":{"nullable":"true"}},{"name":"boolean","type":"boolean","metadata":{}},{"name":"short","type":"short","metadata":{}},{"name":"int","type":"int","metadata":{}},{"name":"long","type":"long","metadata":{}},{"name":"ushort","type":"ushort","metadata":{}},{"name":"uint","type":"uint","metadata":{}},{"name":"ulong","type":"ulong","metadata":{}},{"name":"float","type":"float","metadata":{}},{"name":"double","type":"double","metadata":{}},{"name":"date","type":"date","metadata":{}},{"name":"datetime","type":"datetime","metadata":{}},{"name":"duration","type":"duration","metadata":{}},{"name":"reference","type":"reference","field":"row","metadata":{}},{"name":"string","type":"string","metadata":{}},{"name":"blob","type":"blob","metadata":{}},{"name":"symbol","type":"symbol","metadata":{}},{"name":"structure","type":"structure","fields":[{"name":"string","type":"string","metadata":{}}],"metadata":{}},{"name":"list","type":"list","elements":{"type":"string"},"metadata":{}},{"name":"row","type":"row","fields":[{"name":"string","type":"string","metadata":{}}],"metadata":{}}]}"""
    )
  }

}
