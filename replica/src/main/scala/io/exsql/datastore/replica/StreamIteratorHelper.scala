package io.exsql.datastore.replica

import io.exsql.bytegraph.ByteGraph.ByteGraphRow
import io.exsql.bytegraph.ByteGraphValueType
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, IntervalUtils, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.time.{Instant, ZoneOffset}

class StreamIteratorHelper {

  private final val SECONDS_PER_DAY = 60 * 60 * 24L

  private final val MICROS_PER_MILLIS = 1000L

  private final val MILLIS_PER_SECOND = 1000L

  private final val MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND

  private final val NANOS_PER_MICROS = 1000L

  def iterator(namespace: String, stream: String, partition: String): Iterator[InternalRow] = {
    val streamInstance = NamespaceRegistry.get(namespace).get(stream)
    val schema = streamInstance.schema
    streamInstance.iterator(partition.toInt).map { value =>
      new ByteGraphInternalRow(value.as[ByteGraphRow](), schema)
    }
  }

  private def instantToDays(instant: Instant): Int = {
    val seconds = instant.getEpochSecond
    val days = Math.floorDiv(seconds, SECONDS_PER_DAY)
    days.toInt
  }

  private def instantToMicros(instant: Instant): Long = {
    val sec = Math.multiplyExact(instant.getEpochSecond, MICROS_PER_SECOND)
    val result = Math.addExact(sec, instant.getNano / NANOS_PER_MICROS)
    result
  }

  class ByteGraphInternalRow(row: ByteGraphRow, schema: ByteGraphSchema) extends InternalRow {
    override def numFields: Int = schema.fields.length
    override def isNullAt(ordinal: Int): Boolean = row(ordinal).isEmpty
    override def getBoolean(ordinal: Int): Boolean = row(ordinal).get.as[Boolean]()
    override def getShort(ordinal: Int): Short = row(ordinal).get.as[Short]()
    override def getFloat(ordinal: Int): Float = row(ordinal).get.as[Float]()
    override def getDouble(ordinal: Int): Double = row(ordinal).get.as[Double]()
    override def getBinary(ordinal: Int): Array[Byte] = row(ordinal).get.as[Array[Byte]]()

    override def getInt(ordinal: Int): Int = {
      (schema.fields(ordinal).byteGraphDataType.valueType: @unchecked) match {
        case ByteGraphValueType.Int => row(ordinal).get.as[Int]()
        case ByteGraphValueType.Date =>
          val instant = row(ordinal).get.as[java.time.LocalDate]().atStartOfDay(ZoneOffset.UTC).toInstant
          instantToDays(instant)
      }
    }

    override def getLong(ordinal: Int): Long = {
      (schema.fields(ordinal).byteGraphDataType.valueType: @unchecked) match {
        case ByteGraphValueType.Long => row(ordinal).get.as[Long]()
        case ByteGraphValueType.DateTime =>
          val instant = row(ordinal).get.as[java.time.ZonedDateTime]().withZoneSameInstant(ZoneOffset.UTC).toInstant
          instantToMicros(instant)
      }
    }

    override def getUTF8String(ordinal: Int): UTF8String = {
      UTF8String.fromString(row(ordinal).get.as[java.lang.String]())
    }

    override def getInterval(ordinal: Int): CalendarInterval = {
//      val duration = row(ordinal).get.as[java.time.Duration]()
//      val micro = IntervalUtils.durationToMicros(duration)
//      duration.toDays
//      new CalendarInterval(0, duration.toDaysPart, duration.toMillisPart)
      ???
    }

    override def copy(): InternalRow = new ByteGraphInternalRow(row, schema)

    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = ???
    override def getByte(ordinal: Int): Byte = ???
    override def setNullAt(i: Int): Unit = ???
    override def update(i: Int, value: Any): Unit = ???
    override def getStruct(ordinal: Int, numFields: Int): InternalRow = ???
    override def getArray(ordinal: Int): ArrayData = ???
    override def getMap(ordinal: Int): MapData = ???
    override def get(ordinal: Int, dataType: DataType): AnyRef = ???
  }

  def rows(namespace: String, stream: String, partition: String): Long = {
    NamespaceRegistry.get(namespace).get(stream).count(partition.toInt)
  }

  def bytes(namespace: String, stream: String, partition: String): Long = {
    NamespaceRegistry.get(namespace).get(stream).bytes(partition.toInt)
  }

}
