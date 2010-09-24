package com.twitter.haplocheirus

import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import net.lag.logging.Logger
import jobs.RedisCopy


trait HaplocheirusShard extends Shard {
  @throws(classOf[ShardException]) def append(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def remove(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def merge(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def filter(timeline: String, entries: Seq[Long], maxSearch: Int): Option[Seq[Array[Byte]]]
  @throws(classOf[ShardException]) def get(timeline: String, offset: Int, length: Int, dedupe: Boolean): Option[TimelineSegment]
  @throws(classOf[ShardException]) def getRaw(timeline: String): Option[Seq[Array[Byte]]]
  @throws(classOf[ShardException]) def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean): Option[TimelineSegment]
  @throws(classOf[ShardException]) def store(timeline: String, entries: Seq[Array[Byte]])
  @throws(classOf[ShardException]) def deleteTimeline(timeline: String)
  @throws(classOf[ShardException]) def getKeys(offset: Int, count: Int): Seq[String]
  @throws(classOf[ShardException]) def startCopy(timeline: String)
  @throws(classOf[ShardException]) def doCopy(timeline: String, entries: Seq[Array[Byte]])
}

class HaplocheirusShardAdapter(shard: ReadWriteShard[HaplocheirusShard])
      extends ReadWriteShardAdapter(shard) with HaplocheirusShard {
  private val log = Logger.get(getClass.getName)

  def append(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) = shard.writeOperation(_.append(timeline, entries, onError))
  def remove(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) = shard.writeOperation(_.remove(timeline, entries, onError))
  def merge(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) = shard.writeOperation(_.merge(timeline, entries, onError))
  def store(timeline: String, entries: Seq[Array[Byte]]) = shard.writeOperation(_.store(timeline, entries))
  def deleteTimeline(timeline: String) = shard.writeOperation(_.deleteTimeline(timeline))
  def getKeys(offset: Int, count: Int) = shard.readOperation(_.getKeys(offset, count))
  def startCopy(timeline: String) = shard.writeOperation(_.startCopy(timeline))
  def doCopy(timeline: String, entries: Seq[Array[Byte]]) = shard.writeOperation(_.doCopy(timeline, entries))

  // rebuildable:

  def filter(timeline: String, entries: Seq[Long], maxSearch: Int) = {
    shard.rebuildableReadOperation(_.filter(timeline, entries, maxSearch)) { (shard, destShard) =>
      rebuildTimeline(timeline, shard, destShard)
    }
  }

  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean) = {
    shard.rebuildableReadOperation(_.get(timeline, offset, length, dedupe)) { (shard, destShard) =>
      rebuildTimeline(timeline, shard, destShard)
    }
  }

  def getRaw(timeline: String) = {
    shard.rebuildableReadOperation(_.getRaw(timeline)) { (shard, destShard) =>
      rebuildTimeline(timeline, shard, destShard)
    }
  }

  def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean) = {
    shard.rebuildableReadOperation(_.getRange(timeline, fromId, toId, dedupe)) { (shard, destShard) =>
      rebuildTimeline(timeline, shard, destShard)
    }
  }

  private def rebuildTimeline(timeline: String, sourceShard: HaplocheirusShard, destShard: HaplocheirusShard) {
    log.debug("Rebuilding de-cached timeline %s", timeline)
    Stats.incr("timeline-rebuild")
    RedisCopy.copyTimeline(timeline, sourceShard, destShard)
  }
}
