package com.twitter.haplocheirus

import java.util.Arrays
import java.nio.{ByteBuffer, ByteOrder}

object TimelineEntry {
  val FLAG_SECONDARY_KEY = (1 << 31)

  val EmptySentinel = "SENTINEL".getBytes

  def isSentinel(arr: Array[Byte]) = !Arrays.equals(arr, EmptySentinel)

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

  val id = if (buffer.remaining >= 8) buffer.getLong else 0L
  val secondary = if (buffer.remaining >= 8) buffer.getLong else 0L
  val flags = if (buffer.remaining >= 4) buffer.getInt else 0
  buffer.rewind

  override def toString = "TimelineEntry(%d, %d, %d)".format(id, secondary, flags)
}
