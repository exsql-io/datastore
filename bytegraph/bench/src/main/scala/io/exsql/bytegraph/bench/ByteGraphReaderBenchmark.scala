package io.exsql.bytegraph.bench

import java.util.concurrent.TimeUnit

import ByteGraphReaderBenchmark.BenchmarkState
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphRowType, ByteGraphSchemaField}
import io.exsql.bytegraph.{ByteGraph, ByteGraphReader, ByteGraphValueType}
import io.exsql.bytegraph.metadata.ByteGraphSchema
import org.openjdk.jmh.annotations.{Benchmark, OutputTimeUnit, Scope, State}
import org.openjdk.jmh.infra.Blackhole

object ByteGraphReaderBenchmark {

  @State(Scope.Thread)
  class BenchmarkState {

    val byteGraphSchema: ByteGraphRowType = ByteGraphSchema.rowOf(
      (0 until 100).map { index =>
        ByteGraphSchemaField(s"field_$index", ByteGraphValueType.String)
      }: _*
    )

    val byteGraphLarge: Array[Byte] = {
      val byteGraphRowBuilder = ByteGraph.rowBuilder(byteGraphSchema)
      (0 until 100).foreach { index =>
        byteGraphRowBuilder.appendString(s"$index")
      }

      ByteGraph.render(byteGraphRowBuilder.build(), withVersionFlag = true).bytes()
    }

    val reader: ByteGraphReader = ByteGraph.partialReader(
      readSchema = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("field_0", ByteGraphValueType.String),
        ByteGraphSchemaField("field_5", ByteGraphValueType.String),
        ByteGraphSchemaField("field_25", ByteGraphValueType.String),
        ByteGraphSchemaField("field_32", ByteGraphValueType.String),
        ByteGraphSchemaField("field_61", ByteGraphValueType.String),
        ByteGraphSchemaField("field_87", ByteGraphValueType.String),
        ByteGraphSchemaField("field_98", ByteGraphValueType.String)
      ),
      schema = byteGraphSchema
    )
  }

}

class ByteGraphReaderBenchmark {

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def parseLargeStructureWithSkip(state: BenchmarkState, blackhole: Blackhole): Unit = {
    val row = state.reader.read(state.byteGraphLarge)
    blackhole.consume(row(0))
    blackhole.consume(row(1))
    blackhole.consume(row(2))
    blackhole.consume(row(3))
    blackhole.consume(row(4))
    blackhole.consume(row(5))
    blackhole.consume(row(6))
  }

}
