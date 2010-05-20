package com.twitter.haplocheirus

import java.util.concurrent.Future
import scala.collection.mutable
import com.twitter.gizzard.shards._
import net.lag.configgy.ConfigMap
import org.jredis._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisClient, JRedisPipeline}
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec


class RedisShardFactory(config: ConfigMap) extends ShardFactory[HaplocheirusShard] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[HaplocheirusShard]) = {
    val pipelineSize = config("redis_pipeline").toInt
    new RedisShard(shardInfo, weight, children, pipelineSize)
  }

  def materialize(shardInfo: ShardInfo) {
    // no.
  }
}

class RedisShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[HaplocheirusShard],
                 val pipelineMaxSize: Int)
      extends HaplocheirusShard {

  val DEFAULT_PORT = 6379
  val pipeline = new mutable.ListBuffer[Future[ResponseStatus]]

  lazy val redisClient = {
    val segments = shardInfo.hostname.split(":", 2)
    val connectionSpec = if (segments.length == 2) {
      DefaultConnectionSpec.newSpec(segments(0), segments(1).toInt, 0, null)
    } else {
      DefaultConnectionSpec.newSpec(segments(0), DEFAULT_PORT, 0, null)
    }
    new JRedisPipeline(connectionSpec)
  }

  protected def addPipeline(future: Future[ResponseStatus]) {
    pipeline += future
    while (pipeline.size > pipelineMaxSize) {
      val response = pipeline.remove(0).get
      if (response.isError()) {
        // FIXME: put on error queue.
      }
    }
  }

  def append(entry: Array[Byte], timeline: String) {
    addPipeline(redisClient.rpush(timeline, entry))
  }
}
