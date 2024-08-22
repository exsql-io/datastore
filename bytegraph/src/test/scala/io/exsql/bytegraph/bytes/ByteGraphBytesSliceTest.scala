package io.exsql.bytegraph.bytes

import io.exsql.bytegraph.{ByteArrayUtils, intArrayToByteArray}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ByteGraphBytesSliceTest extends AnyFlatSpec with Matchers {

  private val slice = ByteGraphByteArrayAccessor(
    Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
  ).slice(2, 8)

  "A ByteGraphBytesSlice" should "support reading a byte" in {
    slice.getByte(0) should be (0)
  }

  it should "support reading a random byte" in {
    slice.getByte(4) should be (-1)
  }

  it should "support reading a short" in {
    slice.getShort(3) should be (255)
  }

  it should "support reading an int" in {
    slice.getInt(3) should be (16777215)
  }

  it should "support reading a long" in {
    slice.getLong(0) should be (4294967295L)
  }

  it should "support reading a float" in {
    slice.getFloat(3) should be (java.lang.Float.intBitsToFloat(16777215))
  }

  it should "support reading a double" in {
    slice.getDouble(0) should be (java.lang.Double.longBitsToDouble(4294967295L))
  }

  it should "support reading all bytes" in {
    ByteArrayUtils.toHexString(slice.getBytes(3)) should be ("00ffffffff")
  }

  it should "support reading all bytes at a specific offset" in {
    ByteArrayUtils.toHexString(slice.getBytes(0)) should be ("00000000ffffffff")
  }

  it should "support reading some bytes" in {
    ByteArrayUtils.toHexString(slice.getBytes(0, 4)) should be ("00000000")
  }

  it should "support reading some bytes at a specific offset" in {
    ByteArrayUtils.toHexString(slice.getBytes(2, 3)) should be ("0000ff")
  }

  it should "support getting the size" in {
    slice.size() should be (8)
  }

  it should "support getting a slice" in {
    ByteArrayUtils.toHexString(slice.slice(4).bytes()) should be ("ffffffff")
  }

  it should "support getting a slice with a limit" in {
    ByteArrayUtils.toHexString(slice.slice(4, 2).bytes()) should be ("ffff")
  }

  it should "support read mode" in {
    slice.position() should be (0)
    slice.remainingBytes() should be (8)
    slice.readByte() should be (0x00)

    slice.position() should be (1)
    slice.remainingBytes() should be (7)
    slice.readShort() should be (0)

    slice.position() should be (3)
    slice.remainingBytes() should be (5)
    slice.readInt() should be (16777215)

    slice.position() should be (7)
    slice.remainingBytes() should be (1)
    slice.readByte() should be (-1)

    slice.position() should be (8)
    slice.remainingBytes() should be (0)
  }

}
