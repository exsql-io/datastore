package io.exsql.bytegraph.bytes

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.exsql.bytegraph.{ByteArrayUtils, intArrayToByteArray}

class ByteGraphBytesTest extends AnyFlatSpec with Matchers {

  "A ByteGraphBytes" should "support reading a byte" in {
    ByteGraphByteArrayAccessor(Array(0xFF)).getByte(0) should be (-1)
  }

  it should "support reading a random byte" in {
    ByteGraphByteArrayAccessor(Array(0x00, 0xFF)).getByte(1) should be (-1)
  }

  it should "support reading a short" in {
    ByteGraphByteArrayAccessor(Array(0x00, 0xFF)).getShort(0) should be (255)
  }

  it should "support reading an int" in {
    ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0xFF, 0xFF)).getInt(0) should be (65535)
  }

  it should "support reading a long" in {
    ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF)).getLong(0) should be (
      4294967295L
    )
  }

  it should "support reading a float" in {
    ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0xFF, 0xFF)).getFloat(0) should be (
      java.lang.Float.intBitsToFloat(65535)
    )
  }

  it should "support reading a double" in {
    ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF)).getDouble(0) should be (
      java.lang.Double.longBitsToDouble(4294967295L)
    )
  }

  it should "support reading all bytes" in {
    ByteArrayUtils.toHexString(ByteGraphByteArrayAccessor(Array(0xF0, 0x0F)).getBytes(0)) should be ("f00f")
  }

  it should "support reading all bytes at a specific offset" in {
    ByteArrayUtils.toHexString(ByteGraphByteArrayAccessor(Array(0xF0, 0x0F)).getBytes(1)) should be ("0f")
  }

  it should "support reading some bytes" in {
    ByteArrayUtils.toHexString(ByteGraphByteArrayAccessor(Array(0xF0, 0x0F)).getBytes(0, 1)) should be (
      "f0"
    )
  }

  it should "support reading some bytes at a specific offset" in {
    ByteArrayUtils.toHexString(ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0xFF, 0xFF)).getBytes(1, 3)) should be (
      "00ffff"
    )
  }

  it should "support getting the size" in {
    ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0xFF, 0xFF)).size() should be (4)
  }

  it should "support getting a slice" in {
    ByteArrayUtils.toHexString(
      ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF))
        .slice(4)
        .bytes()
    ) should be ("ffffffff")
  }

  it should "support getting a slice with a limit" in {
    ByteArrayUtils.toHexString(
      ByteGraphByteArrayAccessor(Array(0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF))
        .slice(4, 2)
        .bytes()
    ) should be ("ffff")
  }

}
