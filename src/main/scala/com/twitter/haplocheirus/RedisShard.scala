package com.twitter.haplocheirus

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap


class RedisShardFactory(pool: RedisPool) extends ShardFactory[HaplocheirusShard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    new RedisShard(shardInfo, weight, children, pool)
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val pool: RedisPool)
      extends HaplocheirusShard {

  // FIXME should be in config
  val RANGE_QUERY_PAGE_SIZE = 20

  // FIXME count dupes for stats collecting
  private def dedupeBy(entries: Seq[Array[Byte]], byteOffset: Int): Seq[Array[Byte]] = {
    val seen = new mutable.HashSet[Long]()
    entries.reverse.filter { entry =>
      if (byteOffset < entry.length) {
        val id = ByteBuffer.wrap(entry).order(ByteOrder.LITTLE_ENDIAN).getLong(byteOffset)
        !(seen contains id) && { seen += id; true }
      } else {
        true
      }
    }.reverse
  }

  private def timelineIndexOf(entries: Seq[Array[Byte]], entryId: Long): Int = {
    entries.findIndexOf { entry =>
      ByteBuffer.wrap(entry).order(ByteOrder.LITTLE_ENDIAN).getLong(0) == entryId
    }
  }

  def append(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) {
    pool.withClient(shardInfo.hostname) { client =>
      Stats.timeMicros("redis-op-usec") {
        client.push(timeline, entry, onError)
      }
    }
  }

  def remove(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) {
    pool.withClient(shardInfo.hostname) { client =>
      Stats.timeMicros("redis-op-usec") {
        client.pop(timeline, entry, onError)
      }
    }
  }

  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean): Seq[Array[Byte]] = {
    val entries = pool.withClient(shardInfo.hostname) { client =>
      client.get(timeline, offset, length)
    }
    if (dedupe) {
      dedupeBy(dedupeBy(entries, 0), 8)
    } else {
      dedupeBy(entries, 0)
    }
  }

  // FIXME count how many times we had to get another page.
  def getSince(timeline: String, fromId: Long, dedupe: Boolean): Seq[Array[Byte]] = {
    val entriesSince = pool.withClient(shardInfo.hostname) { client =>
      val entries = new mutable.ArrayBuffer[Array[Byte]]()
      var cursor = 0
      var fromIdIndex = -1
      while (fromIdIndex < 0) {
        val newEntries = client.get(timeline, cursor, RANGE_QUERY_PAGE_SIZE)
        cursor += newEntries.size
        if (newEntries.size == 0) {
          // never found the requested id, so return the entire timeline.
          fromIdIndex = entries.size
        } else {
          entries ++= newEntries
          fromIdIndex = timelineIndexOf(entries, fromId)
        }
      }
      entries.take(fromIdIndex)
    }
    if (dedupe) {
      dedupeBy(dedupeBy(entriesSince, 0), 8)
    } else {
      dedupeBy(entriesSince, 0)
    }
  }

  def store(timeline: String, entries: Seq[Array[Byte]]) {
    pool.withClient(shardInfo.hostname) { client =>
      client.set(timeline, entries)
    }
  }

  def deleteTimeline(timeline: String) {
    pool.withClient(shardInfo.hostname) { client =>
      client.delete(timeline)
    }
  }
}
