package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{CopyJob, CopyJobFactory, CopyJobParser, JobScheduler, JsonCodec, JsonJob}
import com.twitter.gizzard.shards.ShardId
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.TimeConversions._

object RedisCopy {
  type Cursor = Int

  val START = 0
  val END = -1
  val COPY_COUNT = 10000

  def copyTimeline(timeline: String, sourceShard: HaplocheirusShard, destinationShard: HaplocheirusShard) {
    destinationShard.startCopy(timeline)
    sourceShard.getRaw(timeline) match {
      case Some(data) => destinationShard.doCopy(timeline, data)
      case None => destinationShard.deleteTimeline(timeline)
    }
  }
}

class RedisCopyFactory(nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob]) extends CopyJobFactory[HaplocheirusShard] {
  def apply(sourceShardId: ShardId, destinationShardId: ShardId) =
    new RedisCopy(sourceShardId, destinationShardId, RedisCopy.START, RedisCopy.COPY_COUNT, nameServer, scheduler)
}

class RedisCopyParser(nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob]) extends CopyJobParser[HaplocheirusShard] {
  def apply(codec: JsonCodec[JsonJob], attributes: Map[String, Any]) = {
    new RedisCopy(
      ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
      ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
      attributes("cursor").asInstanceOf[AnyVal].toInt,
      attributes("count").asInstanceOf[AnyVal].toInt,
      nameServer, scheduler)
  }
}

class RedisCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: RedisCopy.Cursor,
                count: Int, nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob])
      extends CopyJob[HaplocheirusShard](sourceShardId, destinationShardId, count, nameServer, scheduler) {
  def copyPage(sourceShard: HaplocheirusShard, destinationShard: HaplocheirusShard, count: Int) = {
    val keys = sourceShard.getKeys(cursor, count)
    keys.foreach { key => RedisCopy.copyTimeline(key, sourceShard, destinationShard) }
    Stats.incr("copy", keys.size)
    if (keys.size == 0) {
      None
    } else {
      Some(new RedisCopy(sourceShardId, destinationShardId, cursor + keys.size, count, nameServer, scheduler))
    }
  }

  def serialize = Map("cursor" -> cursor)
}
