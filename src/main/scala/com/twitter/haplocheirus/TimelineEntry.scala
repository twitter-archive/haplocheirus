package com.twitter.haplocheirus

import java.nio.{ByteBuffer, ByteOrder}

object TimelineEntry {
  val FLAG_SECONDARY_KEY = (1 << 31)

  def apply(id: Long, secondary: Long, flags: Int) = {
    val data = new Array[Byte](20)
    val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putLong(id)
    buffer.putLong(secondary)
    buffer.putInt(flags)
    new TimelineEntry(data)
  }

  def apply(data: Array[Byte]) = new TimelineEntry(data)
}

final class TimelineEntry(val data: Array[Byte]) {
  private val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)

  def id = buffer.getLong(0)
  def secondary = buffer.getLong(8)
  def flags = buffer.getInt(16)

  override def toString = "TimelineEntry(%d, %d, %d)".format(id, secondary, flags)
}
