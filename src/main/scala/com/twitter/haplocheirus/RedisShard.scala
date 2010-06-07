package com.twitter.haplocheirus

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable
import com.twitter.gizzard.proxy.ExceptionHandlingProxy
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap


class RedisShardFactory(pool: RedisPool, rangeQueryPageSize: Int) extends ShardFactory[HaplocheirusShard] {
  object RedisExceptionWrappingProxy extends ExceptionHandlingProxy({ e =>
    e match {
      case e: ShardException =>
        e
      case e: Throwable =>
        throw new ShardException(e.toString, e)
    }
  })

  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    RedisExceptionWrappingProxy[HaplocheirusShard](
      new RedisShard(shardInfo, weight, children, pool, rangeQueryPageSize))
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val pool: RedisPool, val rangeQueryPageSize: Int)
      extends HaplocheirusShard {

  case class EntryWithKey(key: Long, entry: Array[Byte])

  private def sortKeyFromEntry(entry: Array[Byte], offset: Int): Long = {
    ByteBuffer.wrap(entry).order(ByteOrder.LITTLE_ENDIAN).getLong(offset)
  }

  private def sortKeyFromEntry(entry: Array[Byte]): Long = sortKeyFromEntry(entry, 0)

  def sortKeysFromEntries(entries: Seq[Array[Byte]]): Seq[EntryWithKey] = {
    entries.map { entry => EntryWithKey(sortKeyFromEntry(entry), entry) }
  }

  private def dedupeBy(entries: Seq[Array[Byte]], byteOffset: Int): Seq[Array[Byte]] = {
    val seen = new mutable.HashSet[Long]()
    entries.reverse.filter { entry =>
      if (byteOffset + 8 <= entry.length) {
        val id = sortKeyFromEntry(entry, byteOffset)
        if (seen contains id) {
          Stats.incr("timeline-dupes")
          false
        } else {
          seen += id
          true
        }
      } else {
        true
      }
    }.reverse
  }

  private def timelineIndexOf(entries: Seq[Array[Byte]], entryId: Long): Int = {
    entries.findIndexOf { sortKeyFromEntry(_) == entryId }
  }

  def append(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) {
    pool.withClient(shardInfo.hostname) { client =>
      client.push(timeline, entry, onError)
    }
  }

  def remove(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) {
    pool.withClient(shardInfo.hostname) { client =>
      client.pop(timeline, entry, onError)
    }
  }

  // this is really inefficient. we should discourage its use.
  def filter(timeline: String, entries: Seq[Array[Byte]]) = {
    val searchKeys = sortKeysFromEntries(entries)
    val timelineEntries = Set(sortKeysFromEntries(pool.withClient(shardInfo.hostname) { client =>
      client.get(timeline, 0, -1)
    }).map { _.key }: _*)
    searchKeys.filter { timelineEntries contains _.key }.map { _.entry }
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

  def getSince(timeline: String, fromId: Long, dedupe: Boolean): Seq[Array[Byte]] = {
    val entriesSince = pool.withClient(shardInfo.hostname) { client =>
      val entries = new mutable.ArrayBuffer[Array[Byte]]()
      var cursor = 0
      var fromIdIndex = -1
      while (fromIdIndex < 0) {
        val newEntries = client.get(timeline, cursor, rangeQueryPageSize)
        cursor += newEntries.size
        if (newEntries.size == 0) {
          // never found the requested id, so return the entire timeline.
          fromIdIndex = entries.size
        } else {
          entries ++= newEntries
          fromIdIndex = timelineIndexOf(entries, fromId)
        }
      }
      if (entries.size > rangeQueryPageSize) {
        Stats.incr("timeline-range-page-miss")
      } else {
        Stats.incr("timeline-range-page-hit")
      }
      entries.take(fromIdIndex)
    }
    if (dedupe) {
      dedupeBy(dedupeBy(entriesSince, 0), 8)
    } else {
      dedupeBy(entriesSince, 0)
    }
  }

  def merge(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) {
    pool.withClient(shardInfo.hostname) { client =>
      val existing = sortKeysFromEntries(client.get(timeline, 0, -1))
      if (existing.size > 0) {
        var i = 0
        var previous: EntryWithKey = null
        sortKeysFromEntries(entries).foreach { insert =>
          while (i < existing.size && existing(i).key > insert.key) {
            previous = existing(i)
            i += 1
          }
          if (i == 0) {
            client.push(timeline, insert.entry, onError)
          } else if (i == existing.size ||
                     (existing(i).key != insert.key && previous.key != insert.key)) {
            client.pushAfter(timeline, previous.entry, insert.entry, onError)
            previous = insert
          }
        }
      }
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
