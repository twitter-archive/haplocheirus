package com.twitter.haplocheirus

import com.twitter.gizzard.shards._


trait HaplocheirusShard extends Shard {
  @throws(classOf[ShardException]) def append(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def remove(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def filter(timeline: String, entries: Seq[Array[Byte]]): Seq[Array[Byte]]
  @throws(classOf[ShardException]) def get(timeline: String, offset: Int, length: Int, dedupe: Boolean): TimelineSegment
  @throws(classOf[ShardException]) def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean): TimelineSegment
  @throws(classOf[ShardException]) def store(timeline: String, entries: Seq[Array[Byte]])
  @throws(classOf[ShardException]) def merge(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def deleteTimeline(timeline: String)
  @throws(classOf[ShardException]) def getKeys(offset: Int, count: Int): Seq[String]
  @throws(classOf[ShardException]) def startCopy(timeline: String)
  @throws(classOf[ShardException]) def doCopy(timeline: String, entries: Seq[Array[Byte]])
}

class HaplocheirusShardAdapter(shard: ReadWriteShard[HaplocheirusShard])
      extends ReadWriteShardAdapter(shard) with HaplocheirusShard {
  def append(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) = shard.writeOperation(_.append(entry, timeline, onError))
  def remove(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) = shard.writeOperation(_.remove(entry, timeline, onError))
  def filter(timeline: String, entries: Seq[Array[Byte]]) = shard.readOperation(_.filter(timeline, entries))
  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean) = shard.readOperation(_.get(timeline, offset, length, dedupe))
  def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean) = shard.readOperation(_.getRange(timeline, fromId, toId, dedupe))
  def store(timeline: String, entries: Seq[Array[Byte]]) = shard.writeOperation(_.store(timeline, entries))
  def merge(timeline: String, entries: Seq[Array[Byte]], onError: Option[Throwable => Unit]) = shard.writeOperation(_.merge(timeline, entries, onError))
  def deleteTimeline(timeline: String) = shard.writeOperation(_.deleteTimeline(timeline))
  def getKeys(offset: Int, count: Int) = shard.readOperation(_.getKeys(offset, count))
  def startCopy(timeline: String) = shard.writeOperation(_.startCopy(timeline))
  def doCopy(timeline: String, entries: Seq[Array[Byte]]) = shard.writeOperation(_.doCopy(timeline, entries))
}
