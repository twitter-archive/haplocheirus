package com.twitter.haplocheirus

import com.twitter.gizzard.shards._


trait HaplocheirusShard extends Shard {
  @throws(classOf[ShardException]) def append(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def remove(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit])
  @throws(classOf[ShardException]) def get(timeline: String, offset: Int, length: Int, dedupe: Boolean): Seq[Array[Byte]]
  @throws(classOf[ShardException]) def getSince(timeline: String, fromId: Long, dedupe: Boolean): Seq[Array[Byte]]
  @throws(classOf[ShardException]) def store(timeline: String, entries: Seq[Array[Byte]])
  @throws(classOf[ShardException]) def deleteTimeline(timeline: String)
}

class HaplocheirusShardAdapter(shard: ReadWriteShard[HaplocheirusShard])
      extends ReadWriteShardAdapter(shard) with HaplocheirusShard {
  def append(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) = shard.writeOperation(_.append(entry, timeline, onError))
  def remove(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) = shard.writeOperation(_.remove(entry, timeline, onError))
  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean) = shard.readOperation(_.get(timeline, offset, length, dedupe))
  def getSince(timeline: String, fromId: Long, dedupe: Boolean) = shard.readOperation(_.getSince(timeline, fromId, dedupe))
  def store(timeline: String, entries: Seq[Array[Byte]]) = shard.writeOperation(_.store(timeline, entries))
  def deleteTimeline(timeline: String) = shard.writeOperation(_.deleteTimeline(timeline))
}
