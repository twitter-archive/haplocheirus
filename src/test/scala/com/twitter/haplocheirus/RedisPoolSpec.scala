package com.twitter.haplocheirus

import com.twitter.gizzard.shards.ShardInfo
import org.jredis.ClientRuntimeException
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RedisPoolSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisPool" should {
    val client = mock[PipelinedRedisClient]
    val poolHealthTracker = new RedisPoolHealthTracker(config.redisConfig.poolHealthTrackerConfig)
    var redisPool: RedisPool = null

    doBefore {
      redisPool = new RedisPool("test", poolHealthTracker, config.redisConfig.readPoolConfig) {
        override def makeClient(hostname: String) = client
      }
    }

    "get" in {
      redisPool.serverMap.size mustEqual 0
      redisPool.get(new ShardInfo("RedisShard", "shard1", "a")) mustEqual client
      redisPool.serverMap.keys.toList mustEqual List("a")
    }

    "giveBack" in {
      expect {
        one(client).alive willReturn true
      }

      redisPool.get(new ShardInfo("RedisShard", "shard1", "a")) mustEqual client
      redisPool.serverMap.keys.toList mustEqual List("a")
      redisPool.giveBack("a", client)
    }

    "toString" in {
      expect {
        one(client).alive willReturn true
      }

      redisPool.toString mustEqual "<RedisPool: >"
      redisPool.get(new ShardInfo("RedisShard", "shard1", "a")) mustEqual client
      redisPool.toString mustEqual "<RedisPool: a>"
      redisPool.giveBack("a", client)
      redisPool.toString mustEqual "<RedisPool: a>"
    }

    "withClient" in {
      "in good times" in {
        expect {
          one(client).alive willReturn true
        }

        redisPool.withClient(new ShardInfo("RedisShard", "shard1", "host1")) { client => 3 } mustEqual 3
        redisPool.toString mustEqual "<RedisPool: host1>"
      }

      "in bad times" in {
        expect {
          one(client).shutdown()
          one(client).alive willReturn false
        }

        redisPool.withClient(new ShardInfo("RedisShard", "shard1", "host1")) { client => throw new ClientRuntimeException("rats."); 3 } must throwA[ClientRuntimeException]
        redisPool.toString mustEqual "<RedisPool: >"
      }
    }
  }
}
