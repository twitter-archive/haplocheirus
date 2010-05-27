package com.twitter.haplocheirus

import scala.collection.mutable
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards._
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap


class RedisShardFactory(config: ConfigMap, queue: ErrorHandlingJobQueue) extends ShardFactory[HaplocheirusShard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    val pipelineSize = config("redis_pipeline").toInt
    val timeout = config("redis_timeout_msec").toInt.milliseconds
    val expiration = config("redis_expiration_sec").toInt.seconds
    new RedisShard(shardInfo, weight, children, pipelineSize, timeout, expiration, queue)
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val pipelineMaxSize: Int, val timeout: Duration, val expiration: Duration,
                 val queue: ErrorHandlingJobQueue)
      extends HaplocheirusShard {

  // FIXME: this is creating a new client each time!
  lazy val redisClient =
    new PipelinedRedisClient(shardInfo.hostname, pipelineMaxSize, timeout, expiration, queue)

  def append(entry: Array[Byte], timeline: String) {
    redisClient.push(timeline, entry, Jobs.Append(entry, List(timeline)))
  }

  def remove(entry: Array[Byte], timeline: String) {
    redisClient.pop(timeline, entry, Jobs.Remove(entry, List(timeline)))
  }

  def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]] = {
    redisClient.get(timeline, offset, length)
  }

  def deleteTimeline(timeline: String) {
    redisClient.delete(timeline)
  }
}
