package com.twitter.haplocheirus

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.proxy.ExceptionHandlingProxyFactory
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger


class RedisShardFactory(readPool: RedisPool, writePool: RedisPool, slowPool: RedisPool,
                        rangeQueryPageSize: Int, timelineTrimConfig: TimelineTrimConfig) extends ShardFactory[HaplocheirusShard] {
  object RedisExceptionWrappingProxy extends ExceptionHandlingProxyFactory[RedisShard]({ (shard, e) =>
    e match {
      case e: ShardException =>
        throw e
      case e: Throwable =>
        throw new ShardException(e.toString, e)
    }
  })

  val trimMap = new TimelineTrimMap(timelineTrimConfig)

  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    RedisExceptionWrappingProxy[HaplocheirusShard](
      new RedisShard(shardInfo, weight, children, readPool, writePool, slowPool, trimMap, rangeQueryPageSize))
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val readPool: RedisPool, val writePool: RedisPool, val slowPool: RedisPool,
                 val trimMap: TimelineTrimMap, val rangeQueryPageSize: Int)
      extends HaplocheirusShard {

  import TimelineEntry.{isSentinel, EmptySentinel}

  // do removes the old, painful way. turn this off once everyone is in haplo.
  val OLD_STYLE = true

  private val log = Logger.get(getClass.getName)

  case class EntryWithKey(key: Long, entry: Array[Byte])

  private def sortKeyFromEntry(entry: Array[Byte], offset: Int): Long = {
    // XXX: why does sorting the sentinel out affect this?
    //if (entry.size < offset + 8 || isSentinel(entry)) {
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

  private def dedupe(entries: Seq[Array[Byte]], useSecondary: Boolean): Seq[Array[Byte]] = {
    val rv = new mutable.ArrayBuffer[Array[Byte]] {
      override def initialSize = entries.size
    }
    val keys = mutable.Set.empty[Long]
    val secondaryKeys = mutable.Set.empty[Long]

    val sorted = Sorting.stableSort(entries, compareEntries(_:Array[Byte], _:Array[Byte]))

    sorted.foreach { entry =>
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

  private def compareEntries(a: Array[Byte], b: Array[Byte]) : Boolean = {
    val ak = sortKeyFromEntry(a, 0)
    val bk = sortKeyFromEntry(b, 0)
    ak < bk
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
    writePool.withClient(shardInfo) { client =>
      entries.foreach { entry =>
        client.push(timeline, entry, onError) { checkTrim(client, timeline, _) }
      }
    }
  }

  def remove(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) {
    if (OLD_STYLE) {
      // painful. do it by id because the client doesn't know the entire entry yet.
      val ids = Set(sortKeysFromEntries(entries).map { _.key }: _*)
      writePool.withClient(shardInfo) { client =>
        val timelineEntries = client.get(timeline, 0, -1).map { TimelineEntry(_) }
        timelineEntries.foreach { entry =>
          if (ids contains entry.id) {
            client.pop(timeline, entry.data, onError)
          }
        }
      }
    } else {
      writePool.withClient(shardInfo) { client =>
        entries.foreach { entry =>
          client.pop(timeline, entry, onError)
        }
      }
    }
  }

  // this is really inefficient. we should discourage its use.
  def filter(timeline: String, entries: Seq[Long], maxSearch: Int): Option[Seq[Array[Byte]]] = {
    val needle = Set(entries: _*)
    val timelineEntries = readPool.withClient(shardInfo) { client =>
      client.get(timeline, 0, maxSearch)
    }

    if (timelineEntries.isEmpty) {
      None
    } else {
      Some(
        timelineEntries filter isSentinel map { TimelineEntry(_) } filter { entry =>
          needle.contains(entry.id) ||
          (entry.flags & TimelineEntry.FLAG_SECONDARY_KEY) != 0 &&
          needle.contains(entry.secondary)
        } map { _.data }
      )
    }
  }

  // this is really inefficient. we should discourage its use.
  def oldFilter(timeline: String, entries: Seq[Array[Byte]], maxSearch: Int): Option[Seq[Array[Byte]]] = {
    filter(timeline, sortKeysFromEntries(entries).map { _.key }, maxSearch)
  }

  private def getAndFilterSentinel(client: PipelinedRedisClient, timeline: String, offset: Int, length: Int) = {
    // since 0 is a magic number, leave it alone.
    val sentinelLength = if (length > 0) { length + 1 } else { length }
    val entries = client.get(timeline, offset, sentinelLength)

    if (entries.isEmpty) {
      None
    } else {
      val filtered = entries filter isSentinel

      // if we fetched too many nodes because we didn't get a sentinel
      // value, throw away the last element.
      if (length > 0 && filtered.size > length) {
        Some(filtered.slice(0, filtered.size - 1))
      } else {
        Some(filtered)
      }
    }
  }

  def get(timeline: String, offset: Int, length: Int, dedupeSecondary: Boolean): Option[TimelineSegment] = {
    Stats.timeMicros("redisshard-get-usec") {
      readPool.withClient(shardInfo) { client =>
        // heuristics for detecting a request for the entire timeline
        if (!(offset == 0 && (length == 800 || length == 3200))) {
          val size = client.size(timeline)
          if (size > 0) {
            // empty and miss look the same to redis, fix that
            val entries = getAndFilterSentinel(client, timeline, offset, length).get.toList
            Some(TimelineSegment(dedupe(entries, dedupeSecondary), size-1))
          } else {
            None
          }
        } else {
          getAndFilterSentinel(client, timeline, offset, length) map { entries =>
            TimelineSegment(dedupe(entries, dedupeSecondary), entries.size)
          }
        }
      }
    }
  }

  def getRaw(timeline: String): Option[Seq[Array[Byte]]] = {
    readPool.withClient(shardInfo) { client =>
      getAndFilterSentinel(client, timeline, 0, -1)
    }
  }

  def getRange(timeline: String, fromId: Long, toId: Long, dedupeSecondary: Boolean): Option[TimelineSegment] = {
    readPool.withClient(shardInfo) { client =>
      val results = client.get(timeline, 0, 600)
      if (results.size > 0) {
        val entries = dedupe(results, dedupeSecondary)

        val lastIndex = entries.size
        val toIdIndex = if (toId > 0) {
          val i = entries.findIndexOf { sortKeyFromEntry(_) < toId }
          if (i >= 0) i else 0
        } else {
          0
        }

        var fromIdIndex = if (fromId >= 0) {
          var f = entries.findIndexOf { sortKeyFromEntry(_) <= fromId }
          if (f >= 0) f else lastIndex
        } else {
          lastIndex
        }

        val filteredEntries = entries.slice(toIdIndex, fromIdIndex) filter isSentinel
        Some(TimelineSegment(filteredEntries, filteredEntries.size))
      } else {
        None
      }
    }
  }

  def merge(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) {
    slowPool.withClient(shardInfo) { client =>
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
    slowPool.withClient(shardInfo) { _.setAtomically(timeline, entries ++ Seq(EmptySentinel)) }
  }

  def deleteTimeline(timeline: String) {
    writePool.withClient(shardInfo) { _.delete(timeline) }
  }

  def getKeys(offset: Int, count: Int) = {
    readPool.withClient(shardInfo) { client =>
      if (offset == 0) {
        client.makeKeyList()
      }
      client.getKeys(offset, count)
    }
  }

  def startCopy(timeline: String) {
    writePool.withClient(shardInfo) { _.setLiveStart(timeline) }
  }

  def doCopy(timeline: String, entries: Seq[Array[Byte]]) {
    writePool.withClient(shardInfo) { _.setLive(timeline, entries filter isSentinel) }
  }
}
