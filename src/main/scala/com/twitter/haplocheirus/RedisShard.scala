package com.twitter.haplocheirus

import scala.collection.mutable
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap


class RedisShardFactory(pool: RedisPool) extends ShardFactory[HaplocheirusShard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    new RedisShard(shardInfo, weight, children, pool)
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val pool: RedisPool)
      extends HaplocheirusShard {

  def append(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) {
    pool.withClient(shardInfo.hostname) { client =>
      Stats.timeMicros("redis-op-usec") {
        client.push(timeline, entry, onError)
      }
    }
  }

  def remove(entry: Array[Byte], timeline: String, onError: Option[Throwable => Unit]) {
    pool.withClient(shardInfo.hostname) { client =>
      Stats.timeMicros("redis-op-usec") {
        client.pop(timeline, entry, onError)
      }
    }
  }

  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean): Seq[Array[Byte]] = {
    pool.withClient(shardInfo.hostname) { client =>
      client.get(timeline, offset, length)
    }
  }

  def deleteTimeline(timeline: String) {
    pool.withClient(shardInfo.hostname) { client =>
      client.delete(timeline)
    }
  }
}
