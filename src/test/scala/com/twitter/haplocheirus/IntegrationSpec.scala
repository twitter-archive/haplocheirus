package com.twitter.haplocheirus

import java.util.concurrent.{Future, TimeUnit}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.scheduler.KestrelMessageQueue
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
import com.twitter.gizzard.thrift.conversions.Sequences._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object IntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "Haplocheirus" should {
    val jredisClient = mock[JRedisPipeline]
    val future = mock[JRedisFutureSupport.FutureLong]
    var service: TimelineStoreService = null

    def errorQueue = {
      service.scheduler(Priority.Write.id).queue.errorQueue.asInstanceOf[KestrelMessageQueue]
    }

    doBefore {
      PipelinedRedisClient.mockedOutJRedisClient = Some(jredisClient)
      service = Haplocheirus(config)

      val shardId = new ShardId("localhost", "dev1")
      service.nameServer.createShard(new ShardInfo(shardId, "com.twitter.haplocheirus.RedisShard", "", "", Busy.Normal))
      service.nameServer.setForwarding(new Forwarding(0, 0, shardId))
      service.nameServer.reload()
    }

    doAfter {
      expect {
        one(jredisClient).quit()
      }

      service.shutdown()
      PipelinedRedisClient.mockedOutJRedisClient = None
    }

    val timeline1 = "home_timeline:109"
    val timeline2 = "home_timeline:77777"
    val data = List(123L).pack

    "perform a basic append" in {
      // tricksy: since the expectations are met in another thread, we have to manually assert
      // that they happened.
      var done = false

      expect {
        one(jredisClient).lpushx(timeline1, data) willReturn future
        one(jredisClient).lpushx(timeline2, data) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn { done = true; 2L }
      }

      service.append(data, List(timeline1, timeline2))
      done must beTrue
    }

    "write to the error log on failure, and retry successfully" in {
      var done = false

      expect {
        one(jredisClient).lpushx(timeline1, data) willReturn future
        one(jredisClient).lpushx(timeline2, data) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) will throwA(new Exception("Oups!"))
      }

      errorQueue.size mustEqual 0
      service.append(data, List(timeline1, timeline2))
      errorQueue.size mustEqual 1

      expect {
        allowing(jredisClient).lpushx(timeline2, data) willReturn future
        allowing(future).get(200L, TimeUnit.MILLISECONDS) willReturn { done = true; 3L }
      }

      service.scheduler.retryErrors()
      done must beTrue
      errorQueue.size mustEqual 0
    }
  }
}
