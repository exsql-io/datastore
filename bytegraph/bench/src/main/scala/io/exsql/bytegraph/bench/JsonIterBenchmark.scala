package io.exsql.bytegraph.bench

import java.util.concurrent.TimeUnit

import com.jsoniter.JsonIterator
import JsonIterBenchmark.BenchmarkState
import org.openjdk.jmh.annotations.{Benchmark, OutputTimeUnit, Scope, State}
import org.openjdk.jmh.infra.Blackhole

object JsonIterBenchmark {

  import com.jsoniter.JsonIterator
  import com.jsoniter.spi.DecodingMode

  @State(Scope.Thread)
  class BenchmarkState {
    JsonIterator.setMode(DecodingMode.DYNAMIC_MODE_AND_MATCH_FIELD_WITH_HASH)
    Jdk8DateTimeSupport.enable()

    val allTypes: String = s"""{"boolean":true,"nullable":null,"int":1,"list":[true],"struct":{"list":[true]},"aDouble":365.56}"""
    val large: String = s"""{${(0 until 100).map(index => s""""field_$index":"$index"""").mkString(",")}}"""
  }

}

class JsonIterBenchmark {

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def parseAllTypesStructure(state: BenchmarkState, blackhole: Blackhole): Unit = {
    val value = JsonIterator.deserialize(state.allTypes, classOf[AllTypes])
    blackhole.consume(value.aBoolean)
    //blackhole.consume(value.nullable)
    blackhole.consume(value.aInt)
    blackhole.consume(value.list)
    blackhole.consume(value.struct)
    blackhole.consume(value.aDouble)
  }

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def parseAllTypesStructureReverse(state: BenchmarkState, blackhole: Blackhole): Unit = {
    val value = JsonIterator.deserialize(state.allTypes, classOf[AllTypes])
    blackhole.consume(value.aDouble)
    blackhole.consume(value.struct)
    blackhole.consume(value.list)
    blackhole.consume(value.aInt)
    //blackhole.consume(value.nullable)
    blackhole.consume(value.aBoolean)
  }

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def parseLargeStructure(state: BenchmarkState, blackhole: Blackhole): Unit = {
    readAll(
      JsonIterator.deserialize(state.large),
      (0 until 100).map(index =>  s"field_$index"),
      blackhole
    )
  }

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def parseLargeStructureReverse(state: BenchmarkState, blackhole: Blackhole): Unit = {
    readAll(
      JsonIterator.deserialize(state.large),
      (0 until 100).map(index =>  s"field_$index").reverse,
      blackhole
    )
  }

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def parseLargeStructureWithSkip(state: BenchmarkState, blackhole: Blackhole): Unit = {
    val value = JsonIterator.deserialize(state.large, classOf[SkipStructure])
    blackhole.consume(value.field_0)
    blackhole.consume(value.field_5)
    blackhole.consume(value.field_25)
    blackhole.consume(value.field_32)
    blackhole.consume(value.field_61)
    blackhole.consume(value.field_87)
    blackhole.consume(value.field_98)
  }

  def readAll(any: com.jsoniter.any.Any, fields: Seq[String], blackhole: Blackhole): Unit = {
    val iterator = fields.iterator
    while (iterator.hasNext) {
      blackhole.consume(any.get(iterator.next()))
    }
  }

}
