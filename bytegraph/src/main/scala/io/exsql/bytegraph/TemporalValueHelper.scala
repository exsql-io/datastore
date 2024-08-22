package io.exsql.bytegraph

import java.time._

import TemporalValueConstants._
import io.exsql.bytegraph.bytes.ByteGraphBytes

object TemporalValueHelper {

  val DateBytes: Int = 3
  val DateTimeBytes: Int = java.lang.Long.BYTES
  val DurationBytes: Int = java.lang.Long.BYTES

  def toLocalDate(bytes: Array[Byte], position: Int = 0): LocalDate = {
    toLocalDate(
      ((bytes(position) & intMask) << 24)
        + ((bytes(position + 1) & intMask) << 16)
        + ((bytes(position + 2) & intMask) << 8)
    )
  }

  private def toLocalDate(dateAsInt: Int): LocalDate = {
    require((validationMask & dateAsInt) == 0, "invalid starting pattern for date type")

    LocalDate.of(
      readYear(dateAsInt),
      readMonth(dateAsInt),
      readDay(dateAsInt)
    )
  }

  def toDateTime(bytes: Array[Byte], position: Int = 0): ZonedDateTime = {
    toDateTime(ByteArrayUtils.toInt(bytes, position), ByteArrayUtils.toInt(bytes, position + Integer.BYTES))
  }

  private def toDateTime(dateHourAsInt: Int, minuteSecondAsInt: Int): ZonedDateTime = {
    require((validationMask & dateHourAsInt) == 0, "invalid starting pattern for date time type")

    ZonedDateTime.of(
      readYear(dateHourAsInt),
      readMonth(dateHourAsInt),
      readDay(dateHourAsInt),
      readHours(dateHourAsInt),
      readMinutes(minuteSecondAsInt),
      readSeconds(minuteSecondAsInt),
      readMicros(minuteSecondAsInt),
      ZoneOffset.UTC
    )
  }

  def toDuration(bytes: Array[Byte], position: Int = 0): Duration = {
    toDuration(ByteArrayUtils.toInt(bytes, position), ByteArrayUtils.toInt(bytes, position + Integer.BYTES))
  }

  private def toDuration(hours: Int, minuteSecondAsInt: Int): Duration = {
    val duration = Duration
      .ofHours(hours.abs.toLong)
      .plusMinutes(readMinutes(minuteSecondAsInt).toLong)
      .plusSeconds(readSeconds(minuteSecondAsInt).toLong)
      .plusNanos(readMicros(minuteSecondAsInt).toLong)

    if (hours < 0) duration.negated()
    else duration
  }

  def localDateToBytes(localDate: LocalDate, byteGraphBytes: ByteGraphBytes): Unit = {
    byteGraphBytes.write(localDateToBytes(
      localDate.getYear,
      localDate.getMonthValue - 1,
      localDate.getDayOfMonth - 1
    ))
  }

  private def localDateToBytes(year: Int, month: Int, day: Int): Array[Byte] = {
    val dateAsByte = (year << yearShift) + (month << monthShift) + (day << dayShift)

    Array(
      (dateAsByte >> 24).toByte,
      (dateAsByte >> 16).toByte,
      (dateAsByte >> 8).toByte
    )
  }

  def zonedDateTimeToBytes(zonedDateTime: ZonedDateTime, byteGraphBytes: ByteGraphBytes): Unit = {
    byteGraphBytes.writeInt(
      (zonedDateTime.getYear << yearShift)
        + (zonedDateTime.getMonthValue - 1 << monthShift)
        + (zonedDateTime.getDayOfMonth - 1 << dayShift)
        + zonedDateTime.getHour
    )

    byteGraphBytes.writeInt(
      (zonedDateTime.getMinute << minutesShift)
        + (zonedDateTime.getSecond << secondsShift)
        + (zonedDateTime.getNano / 1000)
    )
  }

  def durationToBytes(duration: Duration, byteGraphBytes: ByteGraphBytes): Unit = {
    durationToBytes(
      byteGraphBytes,
      duration.toHours,
      duration.toMinutes % 60,
      duration.getSeconds % 60,
      duration.getNano / 1000
    )
  }

  private def durationToBytes(byteGraphBytes: ByteGraphBytes, hours: Long, minutes: Long, seconds: Long, microSeconds: Int): Unit = {
    val minutesSecondsAsInt = (minutes << minutesShift) + (seconds << secondsShift) + microSeconds

    byteGraphBytes.write(Array(
      (hours >> 24).toByte,
      (hours >> 16).toByte,
      (hours >> 8).toByte,
      hours.toByte,
      (minutesSecondsAsInt >> 24).toByte,
      (minutesSecondsAsInt >> 16).toByte,
      (minutesSecondsAsInt >> 8).toByte,
      minutesSecondsAsInt.toByte
    ))
  }

  private def readYear(bytes: Int): Int = {
    val year = (bytes & yearMask) >>> yearShift
    if (year >= yearMax) 0 else year
  }

  private def readMonth(bytes: Int): Int = {
    read(bytes, monthMask, monthShift, monthMax, "month") + 1
  }

  private def readDay(bytes: Int): Int = {
    read(bytes, dayMask, dayShift, dayMax, "day") + 1
  }

  private def readHours(bytes: Int): Int = {
    read(bytes, hoursMask, hoursShift, hoursMax, "hours")
  }

  private def readMinutes(bytes: Int): Int = {
    read(bytes, minutesMask, minutesShift, minutesMax, "minutes")
  }

  private def readSeconds(bytes: Int): Int = {
    read(bytes, secondsMask, secondsShift, secondsMax, "seconds")
  }

  private def readMicros(bytes: Int): Int = {
    read(bytes, microsMask, microsShift, microsMax, "micros") * 1000
  }

  private def read(bytes: Int, mask: Int, shift: Int, max: Int, name: String): Int = {
    val result = (bytes & mask) >>> shift

    if (result > max) throw new IllegalArgumentException(s"Unsupported $name: $result")
    else result
  }

}
