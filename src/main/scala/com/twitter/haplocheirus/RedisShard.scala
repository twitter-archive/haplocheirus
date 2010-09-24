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
import net.lag.logging.Logger


class RedisShardFactory(readPool: RedisPool, writePool: RedisPool, rangeQueryPageSize: Int,
                        timelineTrimConfig: ConfigMap) extends ShardFactory[HaplocheirusShard] {
  object RedisExceptionWrappingProxy extends ExceptionHandlingProxy({ e =>
    e match {
      case e: ShardException =>
        e
      case e: Throwable =>
        throw new ShardException(e.toString, e)
    }
  })

  val trimMap = new TimelineTrimMap(timelineTrimConfig)

  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    RedisExceptionWrappingProxy[HaplocheirusShard](
      new RedisShard(shardInfo, weight, children, readPool, writePool, trimMap, rangeQueryPageSize))
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val readPool: RedisPool, val writePool: RedisPool,
                 val trimMap: TimelineTrimMap, val rangeQueryPageSize: Int)
      extends HaplocheirusShard {
  private val log = Logger.get(getClass.getName)

  case class EntryWithKey(key: Long, entry: Array[Byte])

  private def sortKeyFromEntry(entry: Array[Byte], offset: Int): Long = {
    if (entry.size < offset + 8) {
      0
    } else {
      ByteBuffer.wrap(entry).order(ByteOrder.LITTLE_ENDIAN).getLong(offset)
    }
  }

  private def sortKeyFromEntry(entry: Array[Byte]): Long = sortKeyFromEntry(entry, 0)

  def sortKeysFromEntries(entries: Seq[Array[Byte]]): Seq[EntryWithKey] = {
    entries.map { entry => EntryWithKey(sortKeyFromEntry(entry), entry) }
  }

  private def dedupeBy(entries: Seq[Array[Byte]], byteOffset: Int): Seq[Array[Byte]] = {
    // optimization: usually there are no dupes.
    val uniqs = new mutable.HashSet[Long]()
    entries.foreach { entry =>
      if (byteOffset + 8 <= entry.length) {
        uniqs += sortKeyFromEntry(entry, byteOffset)
      }
    }

    if (uniqs.size == entries.size) {
      entries
    } else {
      val seen = new mutable.HashSet[Long]()
      entries.foldRight(List[Array[Byte]]()) { (entry, newList) =>
        if (byteOffset + 8 <= entry.length) {
          val id = sortKeyFromEntry(entry, byteOffset)
          if (seen contains id) {
            Stats.incr("timeline-dupes")
            newList
          } else {
            seen += id
            entry :: newList
          }
        } else {
          entry :: newList
        }
      }
    }
  }

  private def dedupe(entries: Seq[Array[Byte]], useSecondary: Boolean): Seq[Array[Byte]] = {
    val rv = new mutable.ArrayBuffer[Array[Byte]] {
      override def initialSize = entries.size
    }
    val keys = mutable.Set.empty[Long]
    val secondaryKeys = mutable.Set.empty[Long]

    entries.reverse.foreach { entry =>
      if (entry.size < 20) {
        rv += entry
      } else {
        val timelineEntry = TimelineEntry(entry)
        val entryUseSecondary = useSecondary && (timelineEntry.flags & TimelineEntry.FLAG_SECONDARY_KEY) != 0
        if (keys.contains(timelineEntry.id) ||
            (entryUseSecondary &&
              (secondaryKeys.contains(timelineEntry.secondary) || keys.contains(timelineEntry.secondary)))) {
          // skip
        } else {
          rv += entry
          keys += timelineEntry.id
          if (entryUseSecondary) secondaryKeys += timelineEntry.secondary
        }
      }
    }

    rv.reverse
  }

  private def timelineIndexOf(entries: Seq[Array[Byte]], entryId: Long): Int = {
    entries.findIndexOf { sortKeyFromEntry(_) == entryId }
  }

  private def checkTrim(client: PipelinedRedisClient, timeline: String, size: Long) {
    val (lowerBound, upperBound) = trimMap.getBounds(timeline)
    if (size > upperBound) {
      log.debug("Trimming timeline %s: %d -> %d", timeline, size, lowerBound)
      client.trim(timeline, lowerBound)
    }
  }

  def append(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) {
    writePool.withClient(shardInfo.hostname) { client =>
      entries.foreach { entry =>
        client.push(timeline, entry, onError) { checkTrim(client, timeline, _) }
      }
    }
  }

  def remove(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) {
    writePool.withClient(shardInfo.hostname) { client =>
      entries.foreach { entry =>
        client.pop(timeline, entry, onError)
      }
    }
  }

  // this is really inefficient. we should discourage its use.
  def filter(timeline: String, entries: Seq[Long], maxSearch: Int): Option[Seq[Array[Byte]]] = {
    val timelineEntries = Set(sortKeysFromEntries(readPool.withClient(shardInfo.hostname) { client =>
      client.get(timeline, 0, maxSearch)
    }).map { _.key }: _*)
    if (timelineEntries.isEmpty) {
      None
    } else {
      // FIXME
      None
//      Some(searchKeys.filter { timelineEntries contains _ }.map { _.entry })
    }
  }

  def get(timeline: String, offset: Int, length: Int, dedupeSecondary: Boolean): Option[TimelineSegment] = {
    val (entries, size) = readPool.withClient(shardInfo.hostname) { client =>
      (client.get(timeline, offset, length), client.size(timeline))
    }
    if (size > 0) {
      val dedupedEntries = dedupe(entries, dedupeSecondary)
      Stats.incr("timeline-hit")
      Some(TimelineSegment(dedupedEntries, size))
    } else {
      Stats.incr("timeline-miss")
      None
    }
  }

  def getRaw(timeline: String): Option[Seq[Array[Byte]]] = {
    val rv = readPool.withClient(shardInfo.hostname) { _.get(timeline, 0, -1) }
    if (rv.isEmpty) {
      None
    } else {
      Some(rv)
    }
  }

  def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean): Option[TimelineSegment] = {
    val (entriesSince, size) = readPool.withClient(shardInfo.hostname) { client =>
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
      val toIdIndex = if (toId > 0) {
        val i = timelineIndexOf(entries, toId)
        if (i >= 0) i else 0
      } else 0
      (entries.take(fromIdIndex).drop(toIdIndex), client.size(timeline))
    }
    if (size > 0) {
      val entries = if (dedupe) {
        dedupeBy(dedupeBy(entriesSince, 0), 8)
      } else {
        dedupeBy(entriesSince, 0)
      }
      Stats.incr("timeline-hit")
      Some(TimelineSegment(entries, size))
    } else {
      Stats.incr("timeline-miss")
      None
    }
  }

  def merge(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) {
    writePool.withClient(shardInfo.hostname) { client =>
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
            client.push(timeline, insert.entry, onError) { checkTrim(client, timeline, _) }
          } else if (i == existing.size ||
                     (existing(i).key != insert.key && previous.key != insert.key)) {
            client.pushAfter(timeline, previous.entry, insert.entry, onError) { checkTrim(client, timeline, _) }
            previous = insert
          }
        }
      }
    }
  }

  def store(timeline: String, entries: Seq[Array[Byte]]) {
    writePool.withClient(shardInfo.hostname) { _.setAtomically(timeline, entries) }
  }

  def deleteTimeline(timeline: String) {
    writePool.withClient(shardInfo.hostname) { _.delete(timeline) }
  }

  def getKeys(offset: Int, count: Int) = {
    readPool.withClient(shardInfo.hostname) { client =>
      if (offset == 0) {
        client.makeKeyList()
      }
      client.getKeys(offset, count)
    }
  }

  def startCopy(timeline: String) {
    writePool.withClient(shardInfo.hostname) { _.setLiveStart(timeline) }
  }

  def doCopy(timeline: String, entries: Seq[Array[Byte]]) {
    writePool.withClient(shardInfo.hostname) { _.setLive(timeline, entries) }
  }
}
