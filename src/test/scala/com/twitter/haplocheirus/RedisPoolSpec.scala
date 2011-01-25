package com.twitter.haplocheirus

import org.jredis.ClientRuntimeException
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RedisPoolSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisPool" should {
    val client = mock[PipelinedRedisClient]
    var redisPool: RedisPool = null

    doBefore {
      redisPool = new RedisPool("test", config.redisConfig.readPoolConfig) {
        override def makeClient(hostname: String) = client
      }
    }

    "get" in {
      redisPool.serverMap.size mustEqual 0
      redisPool.get("a") mustEqual client
      redisPool.serverMap.keys.toList mustEqual List("a")
      redisPool.serverMap("a").count mustEqual 1
      redisPool.serverMap("a").available.size mustEqual 0
    }

    "giveBack" in {
      expect {
        one(client).alive willReturn true
      }

      redisPool.get("a") mustEqual client
      redisPool.serverMap.keys.toList mustEqual List("a")
      redisPool.serverMap("a").available.size mustEqual 0
      redisPool.giveBack("a", client)
      redisPool.serverMap("a").available.size mustEqual 1
    }

    "toString" in {
      expect {
        one(client).alive willReturn true
      }

      redisPool.toString mustEqual "<RedisPool: >"
      redisPool.get("a") mustEqual client
      redisPool.toString mustEqual "<RedisPool: a=(0 available, 1 total)>"
      redisPool.giveBack("a", client)
      redisPool.toString mustEqual "<RedisPool: a=(1 available, 1 total)>"
    }

    "withClient" in {
      "in good times" in {
        expect {
          one(client).alive willReturn true
        }

        redisPool.withClient("host1") { client => 3 } mustEqual 3
        redisPool.toString mustEqual "<RedisPool: host1=(1 available, 1 total)>"
      }

      "in bad times" in {
        expect {
          one(client).shutdown()
          one(client).alive willReturn false
        }

        redisPool.withClient("host1") { client => throw new ClientRuntimeException("rats."); 3 } must throwA[ClientRuntimeException]
        redisPool.toString mustEqual "<RedisPool: host1=(0 available, 0 total)>"
      }
    }
  }
}
