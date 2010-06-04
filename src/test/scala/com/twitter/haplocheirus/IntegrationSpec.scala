package com.twitter.haplocheirus

import java.util.concurrent.{Future, TimeUnit}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.scheduler.KestrelMessageQueue
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.thrift.conversions.Sequences._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object IntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "Haplocheirus" should {
    val jredisClient = mock[JRedisPipeline]
    val future = mock[JRedisFutureSupport.FutureStatus]
    var service: TimelineStoreService = null

    def errorQueue = {
      service.scheduler(Priority.Write.id).queue.errorQueue.asInstanceOf[KestrelMessageQueue]
    }

    doBefore {
      PipelinedRedisClient.mockedOutJRedisClient = Some(jredisClient)
      service = Haplocheirus(config)

      val shard = service.nameServer.createShard(new ShardInfo("com.twitter.haplocheirus.RedisShard", "dev1", "localhost"))
      service.nameServer.setForwarding(new Forwarding(0, 0, shard))
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
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn { done = true; ResponseStatus.STATUS_OK }
      }

      service.append(data, List(timeline1, timeline2))
      done must beTrue
    }

    "write to the error log on failure, and retry successfully" in {
      var done = false

      expect {
        one(jredisClient).lpushx(timeline1, data) willReturn future
        one(jredisClient).lpushx(timeline2, data) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn { done = true; new ResponseStatus(ResponseStatus.Code.ERROR, "Oups!") }
      }

      errorQueue.size mustEqual 0
      service.append(data, List(timeline1, timeline2))
      done must beTrue
      errorQueue.size mustEqual 1

      done = false

      expect {
        allowing(jredisClient).lpushx(timeline2, data) willReturn future
        allowing(future).get(200L, TimeUnit.MILLISECONDS) willReturn { done = true; ResponseStatus.STATUS_OK }
      }

      service.scheduler.retryErrors()
      done must beTrue
      errorQueue.size mustEqual 0
    }
  }
}
