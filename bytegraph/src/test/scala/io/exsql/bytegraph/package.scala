package io.exsql

import java.nio.charset.StandardCharsets
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import io.exsql.bytegraph.ByteGraph.ByteGraphValue
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchemaField
import io.exsql.bytegraph.metadata.ByteGraphSchema

package object bytegraph {

  implicit def intArrayToByteArray(ints: Array[Int]): Array[Byte] = ints.map(_.toByte)

  private[bytegraph] val byteGraphRowSchema = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("null", ByteGraphValueType.Null),
    ByteGraphSchemaField("true", ByteGraphValueType.Boolean),
    ByteGraphSchemaField("false", ByteGraphValueType.Boolean),
    ByteGraphSchemaField("short", ByteGraphValueType.Short),
    ByteGraphSchemaField("int", ByteGraphValueType.Int),
    ByteGraphSchemaField("long", ByteGraphValueType.Long),
    ByteGraphSchemaField("uShort", ByteGraphValueType.UShort),
    ByteGraphSchemaField("uInt", ByteGraphValueType.UInt),
    ByteGraphSchemaField("uLong", ByteGraphValueType.ULong),
    ByteGraphSchemaField("float", ByteGraphValueType.Float),
    ByteGraphSchemaField("double", ByteGraphValueType.Double),
    ByteGraphSchemaField("date", ByteGraphValueType.Date),
    ByteGraphSchemaField("dateTime", ByteGraphValueType.DateTime),
    ByteGraphSchemaField("duration", ByteGraphValueType.Duration),
    ByteGraphSchemaField("string", ByteGraphValueType.String),
    ByteGraphSchemaField("blob", ByteGraphValueType.Blob),
    ByteGraphSchemaField("row", ByteGraphSchema.rowOf(
      ByteGraphSchemaField("null", ByteGraphValueType.Null),
      ByteGraphSchemaField("true", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("false", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("short", ByteGraphValueType.Short),
      ByteGraphSchemaField("int", ByteGraphValueType.Int),
      ByteGraphSchemaField("long", ByteGraphValueType.Long)
    )),
    ByteGraphSchemaField("booleans", ByteGraphSchema.listOf(ByteGraphValueType.Boolean)),
    ByteGraphSchemaField("integers", ByteGraphSchema.listOf(ByteGraphValueType.Int)),
    ByteGraphSchemaField("uIntegers", ByteGraphSchema.listOf(ByteGraphValueType.UInt)),
    ByteGraphSchemaField("shorts", ByteGraphSchema.listOf(ByteGraphValueType.Short)),
    ByteGraphSchemaField("uShorts", ByteGraphSchema.listOf(ByteGraphValueType.UShort)),
    ByteGraphSchemaField("longs", ByteGraphSchema.listOf(ByteGraphValueType.Long)),
    ByteGraphSchemaField("uLongs", ByteGraphSchema.listOf(ByteGraphValueType.ULong)),
    ByteGraphSchemaField("floats", ByteGraphSchema.listOf(ByteGraphValueType.Float)),
    ByteGraphSchemaField("doubles", ByteGraphSchema.listOf(ByteGraphValueType.Double)),
    ByteGraphSchemaField("dates", ByteGraphSchema.listOf(ByteGraphValueType.Date)),
    ByteGraphSchemaField("dateTimes", ByteGraphSchema.listOf(ByteGraphValueType.DateTime)),
    ByteGraphSchemaField("durations", ByteGraphSchema.listOf(ByteGraphValueType.Duration)),
    ByteGraphSchemaField("strings", ByteGraphSchema.listOf(ByteGraphValueType.String)),
    ByteGraphSchemaField("blobs", ByteGraphSchema.listOf(ByteGraphValueType.Blob)),
    ByteGraphSchemaField("listOfStrings", ByteGraphSchema.listOf(ByteGraphSchema.listOf(ByteGraphValueType.String))),
    ByteGraphSchemaField("dynamic", ByteGraphValueType.Structure),
    ByteGraphSchemaField("reference", ByteGraphSchema.referenceOf("row"))
  )

  private[bytegraph] val byteGraphStructureSchema = ByteGraphSchema.structureOf(
    ByteGraphSchemaField("null", ByteGraphValueType.Null),
    ByteGraphSchemaField("true", ByteGraphValueType.Boolean),
    ByteGraphSchemaField("false", ByteGraphValueType.Boolean),
    ByteGraphSchemaField("short", ByteGraphValueType.Short),
    ByteGraphSchemaField("int", ByteGraphValueType.Int),
    ByteGraphSchemaField("long", ByteGraphValueType.Long),
    ByteGraphSchemaField("uShort", ByteGraphValueType.UShort),
    ByteGraphSchemaField("uInt", ByteGraphValueType.UInt),
    ByteGraphSchemaField("uLong", ByteGraphValueType.ULong),
    ByteGraphSchemaField("float", ByteGraphValueType.Float),
    ByteGraphSchemaField("double", ByteGraphValueType.Double),
    ByteGraphSchemaField("date", ByteGraphValueType.Date),
    ByteGraphSchemaField("dateTime", ByteGraphValueType.DateTime),
    ByteGraphSchemaField("duration", ByteGraphValueType.Duration),
    ByteGraphSchemaField("string", ByteGraphValueType.String),
    ByteGraphSchemaField("blob", ByteGraphValueType.Blob),
    ByteGraphSchemaField("structure", ByteGraphSchema.structureOf(
      ByteGraphSchemaField("null", ByteGraphValueType.Null),
      ByteGraphSchemaField("true", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("false", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("short", ByteGraphValueType.Short),
      ByteGraphSchemaField("int", ByteGraphValueType.Int),
      ByteGraphSchemaField("long", ByteGraphValueType.Long)
    )),
    ByteGraphSchemaField("booleans", ByteGraphSchema.listOf(ByteGraphValueType.Boolean)),
    ByteGraphSchemaField("integers", ByteGraphSchema.listOf(ByteGraphValueType.Int)),
    ByteGraphSchemaField("uIntegers", ByteGraphSchema.listOf(ByteGraphValueType.UInt)),
    ByteGraphSchemaField("shorts", ByteGraphSchema.listOf(ByteGraphValueType.Short)),
    ByteGraphSchemaField("uShorts", ByteGraphSchema.listOf(ByteGraphValueType.UShort)),
    ByteGraphSchemaField("longs", ByteGraphSchema.listOf(ByteGraphValueType.Long)),
    ByteGraphSchemaField("uLongs", ByteGraphSchema.listOf(ByteGraphValueType.ULong)),
    ByteGraphSchemaField("floats", ByteGraphSchema.listOf(ByteGraphValueType.Float)),
    ByteGraphSchemaField("doubles", ByteGraphSchema.listOf(ByteGraphValueType.Double)),
    ByteGraphSchemaField("dates", ByteGraphSchema.listOf(ByteGraphValueType.Date)),
    ByteGraphSchemaField("dateTimes", ByteGraphSchema.listOf(ByteGraphValueType.DateTime)),
    ByteGraphSchemaField("durations", ByteGraphSchema.listOf(ByteGraphValueType.Duration)),
    ByteGraphSchemaField("strings", ByteGraphSchema.listOf(ByteGraphValueType.String)),
    ByteGraphSchemaField("blobs", ByteGraphSchema.listOf(ByteGraphValueType.Blob)),
    ByteGraphSchemaField("listOfStrings", ByteGraphSchema.listOf(ByteGraphSchema.listOf(ByteGraphValueType.String)))
  )

  def withRow(test: ByteGraphValue => Unit): Unit = {
    test {
      ByteGraph.valueOf(
        ByteGraph.render(
          ByteGraph
            .rowBuilder(byteGraphRowSchema)
            .appendNull()
            .appendBoolean(true)
            .appendBoolean(false)
            .appendShort(Short.MaxValue)
            .appendInt(Int.MaxValue)
            .appendLong(Long.MaxValue)
            .appendUShort(-1)
            .appendUInt(-1)
            .appendULong(-1)
            .appendFloat(Float.MaxValue)
            .appendDouble(Double.MaxValue)
            .appendDate(LocalDate.of(2019, 1, 1))
            .appendDateTime(ZonedDateTime.of(
              LocalDate.of(2019, 1, 1),
              LocalTime.of(23, 59, 59),
              ZoneOffset.UTC
            ))
            .appendDuration(java.time.Duration.ofSeconds(3600))
            .appendString("string")
            .appendBlob("blob".getBytes(StandardCharsets.UTF_8))
            .appendRow {
              _.appendNull()
                .appendBoolean(true)
                .appendBoolean(false)
                .appendShort(Short.MaxValue)
                .appendInt(Int.MaxValue)
                .appendLong(Long.MaxValue)
            }
            .appendList(_.appendBoolean(true).appendBoolean(false).appendBoolean(true))
            .appendList(_.appendInt(Int.MaxValue - 2).appendInt(Int.MaxValue - 1).appendInt(Int.MaxValue))
            .appendList(_.appendUInt(-3).appendUInt(-2).appendUInt(-1))
            .appendList(
              _.appendShort((Short.MaxValue - 2).toShort).appendShort((Short.MaxValue - 1).toShort).appendShort(Short.MaxValue)
            )
            .appendList(_.appendUShort(-3).appendUShort(-2).appendUShort(-1))
            .appendList(_.appendLong(Long.MaxValue - 2).appendLong(Long.MaxValue - 1).appendLong(Long.MaxValue))
            .appendList(_.appendULong(-3).appendULong(-2).appendULong(-1))
            .appendList(_.appendFloat(Float.MaxValue - 2).appendFloat(Float.MaxValue - 1).appendFloat(Float.MaxValue))
            .appendList(_.appendDouble(Double.MaxValue - 2).appendDouble(Double.MaxValue - 1).appendDouble(Double.MaxValue))
            .appendList(
              _.appendDate(LocalDate.of(2019, 1, 1))
                .appendDate(LocalDate.of(2019, 1, 2))
                .appendDate(LocalDate.of(2019, 1, 3))
            )
            .appendList(
              _.appendDateTime(
                ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
              )
                .appendDateTime(
                  ZonedDateTime.of(LocalDate.of(2019, 1, 2), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
                )
                .appendDateTime(
                  ZonedDateTime.of(LocalDate.of(2019, 1, 3), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
                )
            )
            .appendList(
              _.appendDuration(java.time.Duration.ofSeconds(3600))
                .appendDuration(java.time.Duration.ofSeconds(7200))
                .appendDuration(java.time.Duration.ofSeconds(10800))
            )
            .appendList(
              _.appendString("string1")
                .appendString("string2")
                .appendString("string3")
            )
            .appendList(
              _.appendBlob("blob1".getBytes(StandardCharsets.UTF_8))
                .appendBlob("blob2".getBytes(StandardCharsets.UTF_8))
                .appendBlob("blob3".getBytes(StandardCharsets.UTF_8))
            )
            .appendList(
              _.appendList(
                _.appendString("string1")
                  .appendString("string2")
                  .appendString("string3")
              )
                .appendList(
                  _.appendString("string4")
                    .appendString("string5")
                    .appendString("string6")
                )
                .appendList(
                  _.appendString("string7")
                    .appendString("string8")
                    .appendString("string9")
                )
            )
            .appendStructure { structureBuilder =>
              structureBuilder.appendString("field", "value")
            }
            .appendReference("row")
            .build(),
          withVersionFlag = true
        ),
        Some(byteGraphRowSchema)
      )
    }
  }

  def withList(test: ByteGraphValue => Unit): Unit = {
    test {
      ByteGraph.valueOf(
        ByteGraph.render(
          ByteGraph
            .listBuilder()
            .appendNull()
            .appendBoolean(true)
            .appendBoolean(false)
            .appendShort(Short.MaxValue)
            .appendInt(Int.MaxValue)
            .appendLong(Long.MaxValue)
            .appendUShort(-1)
            .appendUInt(-1)
            .appendULong(-1)
            .appendFloat(Float.MaxValue)
            .appendDouble(Double.MaxValue)
            .appendDate(LocalDate.of(2019, 1, 1))
            .appendDateTime(ZonedDateTime.of(
              LocalDate.of(2019, 1, 1),
              LocalTime.of(23, 59, 59),
              ZoneOffset.UTC
            ))
            .appendDuration(java.time.Duration.ofSeconds(3600))
            .appendString("string")
            .appendBlob("blob".getBytes(StandardCharsets.UTF_8))
            .appendList(_.appendBoolean(true).appendBoolean(false).appendBoolean(true))
            .appendList(_.appendInt(Int.MaxValue - 2).appendInt(Int.MaxValue - 1).appendInt(Int.MaxValue))
            .appendList(_.appendUInt(-3).appendUInt(-2).appendUInt(-1))
            .appendList(
              _.appendShort((Short.MaxValue - 2).toShort).appendShort((Short.MaxValue - 1).toShort).appendShort(Short.MaxValue)
            )
            .appendList(_.appendUShort(-3).appendUShort(-2).appendUShort(-1))
            .appendList(_.appendLong(Long.MaxValue - 2).appendLong(Long.MaxValue - 1).appendLong(Long.MaxValue))
            .appendList(_.appendULong(-3).appendULong(-2).appendULong(-1))
            .appendList(_.appendFloat(Float.MaxValue - 2).appendFloat(Float.MaxValue - 1).appendFloat(Float.MaxValue))
            .appendList(_.appendDouble(Double.MaxValue - 2).appendDouble(Double.MaxValue - 1).appendDouble(Double.MaxValue))
            .appendList(
              _.appendDate(LocalDate.of(2019, 1, 1))
                .appendDate(LocalDate.of(2019, 1, 2))
                .appendDate(LocalDate.of(2019, 1, 3))
            )
            .appendList(
              _.appendDateTime(
                ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
              )
                .appendDateTime(
                  ZonedDateTime.of(LocalDate.of(2019, 1, 2), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
                )
                .appendDateTime(
                  ZonedDateTime.of(LocalDate.of(2019, 1, 3), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
                )
            )
            .appendList(
              _.appendDuration(java.time.Duration.ofSeconds(3600))
                .appendDuration(java.time.Duration.ofSeconds(7200))
                .appendDuration(java.time.Duration.ofSeconds(10800))
            )
            .appendList(
              _.appendString("string1")
                .appendString("string2")
                .appendString("string3")
            )
            .appendList(
              _.appendBlob("blob1".getBytes(StandardCharsets.UTF_8))
                .appendBlob("blob2".getBytes(StandardCharsets.UTF_8))
                .appendBlob("blob3".getBytes(StandardCharsets.UTF_8))
            )
            .appendList(
              _.appendList(
                _.appendString("string1")
                  .appendString("string2")
                  .appendString("string3")
              )
                .appendList(
                  _.appendString("string4")
                    .appendString("string5")
                    .appendString("string6")
                )
                .appendList(
                  _.appendString("string7")
                    .appendString("string8")
                    .appendString("string9")
                )
            )
            .build(),
          withVersionFlag = true
        )
      )
    }
  }

  def withStructure(test: ByteGraphValue => Unit): Unit = {
    test {
      ByteGraph.valueOf(
        ByteGraph.render(
          ByteGraph
            .structureBuilder()
            .appendNull("null")
            .appendBoolean("true", value = true)
            .appendBoolean("false", value = false)
            .appendShort("short", Short.MaxValue)
            .appendInt("int", Int.MaxValue)
            .appendLong("long", Long.MaxValue)
            .appendUShort("uShort", -1)
            .appendUInt("uInt", -1)
            .appendULong("uLong", -1)
            .appendFloat("float", Float.MaxValue)
            .appendDouble("double", Double.MaxValue)
            .appendDate("date", LocalDate.of(2019, 1, 1))
            .appendDateTime("dateTime", ZonedDateTime.of(
              LocalDate.of(2019, 1, 1),
              LocalTime.of(23, 59, 59),
              ZoneOffset.UTC
            ))
            .appendDuration("duration", java.time.Duration.ofSeconds(3600))
            .appendString("string", "string")
            .appendBlob("blob", "blob".getBytes(StandardCharsets.UTF_8))
            .appendStructure("structure") {
              _.appendNull("null")
               .appendBoolean("true", value = true)
               .appendBoolean("false", value = false)
               .appendShort("short", Short.MaxValue)
               .appendInt("int", Int.MaxValue)
               .appendLong("long", Long.MaxValue)
            }
            .appendList("booleans")(_.appendBoolean(true).appendBoolean(false).appendBoolean(true))
            .appendList("integers")(_.appendInt(Int.MaxValue - 2).appendInt(Int.MaxValue - 1).appendInt(Int.MaxValue))
            .appendList("uIntegers")(_.appendUInt(-3).appendUInt(-2).appendUInt(-1))
            .appendList("shorts")(
              _.appendShort((Short.MaxValue - 2).toShort).appendShort((Short.MaxValue - 1).toShort).appendShort(Short.MaxValue)
            )
            .appendList("uShorts")(_.appendUShort(-3).appendUShort(-2).appendUShort(-1))
            .appendList("longs")(_.appendLong(Long.MaxValue - 2).appendLong(Long.MaxValue - 1).appendLong(Long.MaxValue))
            .appendList("uLongs")(_.appendULong(-3).appendULong(-2).appendULong(-1))
            .appendList("floats")(_.appendFloat(Float.MaxValue - 2).appendFloat(Float.MaxValue - 1).appendFloat(Float.MaxValue))
            .appendList("doubles")(_.appendDouble(Double.MaxValue - 2).appendDouble(Double.MaxValue - 1).appendDouble(Double.MaxValue))
            .appendList("dates")(
              _.appendDate(LocalDate.of(2019, 1, 1))
               .appendDate(LocalDate.of(2019, 1, 2))
               .appendDate(LocalDate.of(2019, 1, 3))
            )
            .appendList("dateTimes")(
              _.appendDateTime(
                ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
              )
              .appendDateTime(
                ZonedDateTime.of(LocalDate.of(2019, 1, 2), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
              )
              .appendDateTime(
                ZonedDateTime.of(LocalDate.of(2019, 1, 3), LocalTime.of(23, 59, 59), ZoneOffset.UTC)
              )
            )
            .appendList("durations")(
              _.appendDuration(java.time.Duration.ofSeconds(3600))
               .appendDuration(java.time.Duration.ofSeconds(7200))
               .appendDuration(java.time.Duration.ofSeconds(10800))
            )
            .appendList("strings")(
              _.appendString("string1")
               .appendString("string2")
               .appendString("string3")
            )
            .appendList("blobs")(
              _.appendBlob("blob1".getBytes(StandardCharsets.UTF_8))
               .appendBlob("blob2".getBytes(StandardCharsets.UTF_8))
               .appendBlob("blob3".getBytes(StandardCharsets.UTF_8))
            )
            .appendList("listOfStrings")(
              _.appendList(
                _.appendString("string1")
                 .appendString("string2")
                 .appendString("string3")
              )
              .appendList(
                _.appendString("string4")
                 .appendString("string5")
                 .appendString("string6")
              )
              .appendList(
                _.appendString("string7")
                 .appendString("string8")
                 .appendString("string9")
              )
            )
            .build(),
          withVersionFlag = true
        )
      )
    }
  }

}
