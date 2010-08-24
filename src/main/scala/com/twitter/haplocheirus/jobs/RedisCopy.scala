package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.jobs.{Copy, CopyFactory, CopyParser}
import com.twitter.gizzard.shards.ShardId
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.TimeConversions._


object RedisCopy {
  type Cursor = Int

  val START = 0
  val END = -1
  val COPY_COUNT = 10000
}

object RedisCopyFactory extends CopyFactory[HaplocheirusShard] {
  def apply(sourceShardId: ShardId, destinationShardId: ShardId) =
    new RedisCopy(sourceShardId, destinationShardId, RedisCopy.START)
}

object RedisCopyParser extends CopyParser[HaplocheirusShard] {
  def apply(attributes: Map[String, Any]) = {
    new RedisCopy(
      ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
      ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
      attributes("cursor").asInstanceOf[AnyVal].toInt,
      attributes("count").asInstanceOf[AnyVal].toInt)
  }
}

class RedisCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: RedisCopy.Cursor, count: Int)
      extends Copy[HaplocheirusShard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: RedisCopy.Cursor) =
    this(sourceShardId, destinationShardId, cursor, RedisCopy.COPY_COUNT)

  def copyPage(sourceShard: HaplocheirusShard, destinationShard: HaplocheirusShard, count: Int) = {
    val keys = sourceShard.getKeys(cursor, count)
    keys.foreach { key =>
      destinationShard.startCopy(key)
      sourceShard.getRaw(key) match {
        case Some(data) => destinationShard.doCopy(key, data)
        case None => destinationShard.deleteTimeline(key)
      }
    }
    Stats.incr("copy", keys.size)
    if (keys.size == 0) {
      None
    } else {
      Some(new RedisCopy(sourceShardId, destinationShardId, cursor + keys.size, count))
    }
  }

  def serialize = Map("cursor" -> cursor)
}
