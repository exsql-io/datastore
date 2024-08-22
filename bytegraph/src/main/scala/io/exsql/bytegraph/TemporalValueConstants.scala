package io.exsql.bytegraph

object TemporalValueConstants {

  private[bytegraph] val intMask = 0xFF

  // 1110 0000 0000 0000 0000 0000 1110 0000
  private[bytegraph] val validationMask = 0xE00000E0

  // 0001 1111 1111 1110 0000 0000 0000 0000
  private[bytegraph] val yearMask = 0x1FFE0000

  // 0000 0000 0000 0001 1110 0000 0000 0000
  private[bytegraph] val monthMask = 0x0001E000

  // 0000 0000 0000 0000 0001 1111 0000 0000
  private[bytegraph] val dayMask = 0x00001F00

  // 0000 0000 0000 0000 0000 0000 0001 1111
  private[bytegraph] val hoursMask = 0x000001F

  // 1111 1100 0000 0000 0000 0000 0000 0000
  private[bytegraph] val minutesMask = 0xFC000000

  // 0000 0011 1111 0000 0000 0000 0000 0000
  private[bytegraph] val secondsMask = 0x03F00000

  // 0000 0000 0000 1111 1111 1111 1111 1111
  private[bytegraph] val microsMask = 0x000FFFFF

  private[bytegraph] val yearShift = 17
  private[bytegraph] val monthShift = 13
  private[bytegraph] val dayShift = 8
  private[bytegraph] val hoursShift = 0

  private[bytegraph] val minutesShift = 26
  private[bytegraph] val secondsShift = 20
  private[bytegraph] val microsShift = 0

  private[bytegraph] val yearMax = 4095
  private[bytegraph] val monthMax = 11
  private[bytegraph] val dayMax = 30
  private[bytegraph] val hoursMax = 23
  private[bytegraph] val minutesMax = 59
  private[bytegraph] val secondsMax = 59
  private[bytegraph] val microsMax = 999999

}