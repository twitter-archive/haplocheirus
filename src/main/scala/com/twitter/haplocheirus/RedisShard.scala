package com.twitter.haplocheirus

import scala.collection.mutable
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards._
import net.lag.configgy.ConfigMap


class RedisShardFactory(config: ConfigMap, queue: ErrorHandlingJobQueue) extends ShardFactory[HaplocheirusShard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    val pipelineSize = config("redis_pipeline").toInt
    new RedisShard(shardInfo, weight, children, pipelineSize, queue)
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val pipelineMaxSize: Int, val queue: ErrorHandlingJobQueue)
      extends HaplocheirusShard {

  // FIXME: this is creating a new client each time!
  lazy val redisClient = new PipelinedRedisClient(shardInfo.hostname, pipelineMaxSize, queue)

  def append(entry: Array[Byte], timeline: String) {
    redisClient.execute(Jobs.Append(entry, List(timeline))) { _.rpushx(timeline, entry) }
  }

  def remove(entry: Array[Byte], timeline: String) {
    redisClient.execute(Jobs.Remove(entry, List(timeline))) { _.ldelete(timeline, entry) }
  }
}
