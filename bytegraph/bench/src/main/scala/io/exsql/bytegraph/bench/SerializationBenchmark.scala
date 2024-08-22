package io.exsql.bytegraph.bench

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit

import com.jsoniter.output.JsonStream
import SerializationBenchmark.BenchmarkState
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphListType, ByteGraphRowType, ByteGraphSchemaField}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType, ByteGraphWriter}
import io.exsql.bytegraph.metadata.ByteGraphSchema
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object SerializationBenchmark {

  import com.jsoniter.output.{EncodingMode, JsonStream}

  JsonStream.setMode(EncodingMode.DYNAMIC_MODE)

  @State(Scope.Thread)
  class BenchmarkState {
    val rowSchema: ByteGraphRowType = ByteGraphSchema.rowOf(
      ByteGraphSchemaField("boolean", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("nullable", ByteGraphValueType.Null),
      ByteGraphSchemaField("int", ByteGraphValueType.Int),
      ByteGraphSchemaField("list", ByteGraphListType(ByteGraphValueType.Boolean)),
      ByteGraphSchemaField("struct", ByteGraphSchema.rowOf(ByteGraphSchemaField("list", ByteGraphListType(ByteGraphValueType.Boolean)))),
      ByteGraphSchemaField("é", ByteGraphValueType.Long)
    )

    val dataSet: Map[String, Any] = Map(
      "boolean" -> true,
      "nullable" -> null,
      "int" -> 1,
      "list" -> List(true),
      "struct" -> Map("list" -> List(true)),
      "é" -> 100L
    )

    val row: Array[AnyRef] = Array(
      java.lang.Boolean.TRUE,
      null,
      java.lang.Integer.valueOf(1),
      Array("string"),
//      Array(Array("string")),
      java.lang.Long.valueOf(100L)
    )

    val writer: ByteGraphWriter = ByteGraph.writer(ByteGraphSchema.rowOf(
      ByteGraphSchemaField("boolean", ByteGraphValueType.Boolean),
      ByteGraphSchemaField("nullable", ByteGraphValueType.Null),
      ByteGraphSchemaField("int", ByteGraphValueType.Int),
      ByteGraphSchemaField("list", ByteGraphListType(ByteGraphValueType.String)),
      //      ByteGraphSchemaField("struct", ByteGraphSchema.rowOf(ByteGraphSchemaField("list", ByteGraphListType(ByteGraphValueType.Boolean)))),
      ByteGraphSchemaField("é", ByteGraphValueType.Long)
    ))

    val hugeRow: Array[String] = (1 to 500).map(index => s"value_$index").toArray

    val hugeRowWriter: ByteGraphWriter = ByteGraph.writer(
      ByteGraphSchema.rowOf((1 to 500).map(index => ByteGraphSchemaField(s"field_$index", ByteGraphValueType.String)): _*)
    )
  }

}

class SerializationBenchmark {

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def serializeJsonIter(state: BenchmarkState, blackhole: Blackhole): Unit = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val jsonStream = new JsonStream(byteArrayOutputStream, 256)
    jsonStream.writeObjectStart()

    state.dataSet.foreach { case (key, value) =>
      jsonStream.writeObjectField(key)
      jsonStream.writeVal(value)
    }

    jsonStream.writeObjectEnd()
    jsonStream.close()

    blackhole.consume(byteArrayOutputStream.toByteArray)
  }

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def byteGraphRowBuilder(state: BenchmarkState, blackhole: Blackhole): Unit = {
    val byteGraphRowBuilder = ByteGraph.rowBuilder(state.rowSchema)

    state.dataSet.foreach { case (_, value) =>
      value match {
        case null => byteGraphRowBuilder.appendNull()
        case boolean: Boolean => byteGraphRowBuilder.appendBoolean(boolean)
        case int: Int => byteGraphRowBuilder.appendInt(int)
        case long: Long => byteGraphRowBuilder.appendLong(long)
        case list: List[Any] =>
          val booleans = list.asInstanceOf[List[Boolean]]
          byteGraphRowBuilder.appendList { listBuilder =>
            booleans.foreach(listBuilder.appendBoolean)
          }

        case map: Map[Any, Any] =>
          val nested = map.asInstanceOf[Map[String, List[Boolean]]]
          byteGraphRowBuilder.appendRow { structureBuilder =>
            nested.foreach { case (_, list) =>
              structureBuilder.appendList { listBuilder =>
                list.foreach(listBuilder.appendBoolean)
              }
            }
          }
      }
    }

    blackhole.consume(byteGraphRowBuilder.build())
  }

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def byteGraphRowWriter(state: BenchmarkState, blackhole: Blackhole): Unit = {
    blackhole.consume(state.writer.write(state.row, true))
  }

  @Benchmark @OutputTimeUnit(TimeUnit.SECONDS)
  def byteGraphHugeRowWriter(state: BenchmarkState, blackhole: Blackhole): Unit = {
    blackhole.consume(state.hugeRowWriter.write(state.hugeRow.asInstanceOf[Array[AnyRef]], true))
  }

}
