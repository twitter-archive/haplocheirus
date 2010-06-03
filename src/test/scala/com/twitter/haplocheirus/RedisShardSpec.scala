package com.twitter.haplocheirus

import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.configgy.Configgy
import org.jredis.ClientRuntimeException
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RedisShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisShard" should {
    val client = mock[PipelinedRedisClient]
    val shardInfo = mock[ShardInfo]
    var redisPool: RedisPool = null
    var redisShard: RedisShard = null

    val config = Configgy.config.configMap("redis")
    val data = "hello".getBytes
    val timeline = "t1"

    doBefore {
      redisPool = new RedisPool(config) {
        override def withClient[T](hostname: String)(f: PipelinedRedisClient => T): T = f(client)
      }
      redisShard = new RedisShard(shardInfo, 1, Nil, redisPool)
    }

    "append" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(client).push(timeline, data, None)
      }

      redisShard.append(data, timeline, None)
    }

    "remove" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(client).pop(timeline, data, None)
      }

      redisShard.remove(data, timeline, None)
    }

    "get" in {
      "unique entries" in {
        val entry1 = List(23L).pack
        val entry2 = List(20L).pack
        val entry3 = List(19L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, 10) willReturn List(entry1, entry2, entry3)
        }

        redisShard.get(timeline, 0, 10, false) mustEqual List(entry1, entry2, entry3)
      }

      "with duplicates in the sort key" in {

      }

      "with duplicates in the dedupe key" in {

      }
    }

    "deleteTimeline" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(client).delete(timeline)
      }

      redisShard.deleteTimeline(timeline)
    }
  }
}
