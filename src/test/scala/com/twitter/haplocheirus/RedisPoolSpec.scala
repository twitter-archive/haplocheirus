package com.twitter.haplocheirus

import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import net.lag.configgy.Configgy
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RedisPoolSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisPool" should {
    val queue = mock[ErrorHandlingJobQueue]
    val client = mock[PipelinedRedisClient]
    var redisPool: RedisPool = null
    val config = Configgy.config.configMap("redis")

    doBefore {
      redisPool = new RedisPool(config) {
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
      redisPool.get("a") mustEqual client
      redisPool.serverMap.keys.toList mustEqual List("a")
      redisPool.serverMap("a").available.size mustEqual 0
      redisPool.giveBack("a", client)
      redisPool.serverMap("a").available.size mustEqual 1
    }

    "toString" in {
      redisPool.toString mustEqual "<RedisPool: >"
      redisPool.get("a") mustEqual client
      redisPool.toString mustEqual "<RedisPool: a=(0 available, 1 total)>"
      redisPool.giveBack("a", client)
      redisPool.toString mustEqual "<RedisPool: a=(1 available, 1 total)>"
    }
  }
}
