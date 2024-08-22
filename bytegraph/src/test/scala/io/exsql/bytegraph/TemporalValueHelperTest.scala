package io.exsql.bytegraph

import io.exsql.bytegraph.bytes.InMemoryByteGraphBytes
import java.time._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TemporalValueHelperTest extends AnyFlatSpec with Matchers {

  "A TemporalValueHelper" should "be able to serialize a local date and deserialize it properly" in {
    val localDate = LocalDate.of(2018, 1, 1)
    val byteGraphBytes = InMemoryByteGraphBytes()
    TemporalValueHelper.localDateToBytes(localDate, byteGraphBytes)

    TemporalValueHelper.toLocalDate(byteGraphBytes.getBytes(0)) should be (localDate)
  }

  it should "be able to serialize a zoned date time and deserialize it properly" in {
    val zonedDateTime = ZonedDateTime.of(
      LocalDate.of(2018, 1, 1),
      LocalTime.of(23, 59, 59),
      ZoneOffset.UTC
    )

    val byteGraphBytes = InMemoryByteGraphBytes()
    TemporalValueHelper.zonedDateTimeToBytes(zonedDateTime, byteGraphBytes)

    TemporalValueHelper.toDateTime(byteGraphBytes.getBytes(0)) should be (zonedDateTime)
  }

  it should "be able to serialize a duration and deserialize it properly" in {
    val duration = Duration.ofSeconds(250)
    val byteGraphBytes = InMemoryByteGraphBytes()
    TemporalValueHelper.durationToBytes(duration, byteGraphBytes)

    TemporalValueHelper.toDuration(byteGraphBytes.getBytes(0)) should be (duration)
  }

}
