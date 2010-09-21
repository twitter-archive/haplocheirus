package com.twitter.haplocheirus

import java.util.concurrent.{Future, TimeUnit}
import java.util.{List => JList}
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
    val future2 = mock[JRedisFutureSupport.FutureLong]
    val timelineFuture = mock[Future[JList[Array[Byte]]]]
    var service: TimelineStoreService = null

    def errorQueue = {
      service.scheduler(Priority.Write.id).queue.errorQueue.asInstanceOf[KestrelMessageQueue]
    }

    val shardId1 = new ShardId("localhost", "dev1a")
    val shardId2 = new ShardId("localhost", "dev1b")
    val shardIdR = new ShardId("localhost", "dev1")

    doBefore {
      PipelinedRedisClient.mockedOutJRedisClient = Some(jredisClient)
      service = Haplocheirus(config)

      service.nameServer.createShard(new ShardInfo(shardId1, "com.twitter.haplocheirus.RedisShard", "", "", Busy.Normal))
      service.nameServer.createShard(new ShardInfo(shardId2, "com.twitter.haplocheirus.RedisShard", "", "", Busy.Normal))
      service.nameServer.createShard(new ShardInfo(shardIdR, "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal))
      service.nameServer.addLink(shardIdR, shardId1, 1)
      service.nameServer.setForwarding(new Forwarding(0, 0, shardIdR))
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
        one(jredisClient).rpushx(timeline1, data) willReturn future
        one(jredisClient).rpushx(timeline2, data) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn { done = true; 2L }
      }

      service.append(data, "home_timeline:", List(109L, 77777L))
      done must beTrue
    }

    "write to the error log on failure, and retry successfully" in {
      var done = false

      expect {
        one(jredisClient).rpushx(timeline1, data) willReturn future
        one(jredisClient).rpushx(timeline2, data) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) will throwA(new Exception("Oups!"))
      }

      errorQueue.size mustEqual 0
      service.append(data, "home_timeline:", List(109L, 77777L))
      errorQueue.size mustEqual 1

      expect {
        allowing(jredisClient).lpushx(timeline2, data) willReturn future
        allowing(future).get(200L, TimeUnit.MILLISECONDS) willReturn { done = true; 3L }
      }

      service.scheduler.retryErrors()
      done must beTrue
      errorQueue.size mustEqual 0
    }

    "rebuild one shard from another" in {
      service.nameServer.addLink(shardIdR, shardId2, 1)
      service.nameServer.reload()

      expect {
        one(jredisClient).lrange(timeline1, -2, -1) willReturn timelineFuture
        one(jredisClient).llen(timeline1) willReturn future
        one(timelineFuture).get(200L, TimeUnit.MILLISECONDS) willReturn List[Array[Byte]]().toJavaList
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 0L
        one(jredisClient).lrange(timeline1, -2, -1) willReturn timelineFuture
        one(jredisClient).llen(timeline1) willReturn future
        one(timelineFuture).get(200L, TimeUnit.MILLISECONDS) willReturn List("a", "b").map { _.getBytes }.toJavaList
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 2L
        one(jredisClient).expire(timeline1, 86400) willReturn future2
        one(future2).get(200L, TimeUnit.MILLISECONDS) willReturn 0L

        one(jredisClient).lrange(timeline1, 0, -1) willReturn timelineFuture
        one(timelineFuture).get(200L, TimeUnit.MILLISECONDS) willReturn List("a", "b", "c").map { _.getBytes }.toJavaList
        one(jredisClient).del(timeline1)
        one(jredisClient).rpush(timeline1, new Array[Byte](0))
        one(jredisClient).lpushx(timeline1, "c".getBytes)
        one(jredisClient).lpushx(timeline1, "b".getBytes)
        one(jredisClient).lpushx(timeline1, "a".getBytes)
        one(jredisClient).lrem(timeline1, new Array[Byte](0), 1) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 0L
        one(jredisClient).expire(timeline1, 86400) willReturn future2
        one(future2).get(200L, TimeUnit.MILLISECONDS) willReturn 0L
        one(jredisClient).expire(timeline1, 86400) willReturn future2
        one(future2).get(200L, TimeUnit.MILLISECONDS) willReturn 0L
      }

      val segment = service.get(timeline1, 0, 2, false).get
      segment.size mustEqual 2
      segment.entries(0).toList mustEqual "b".getBytes.toList
      segment.entries(1).toList mustEqual "a".getBytes.toList
    }
  }
}
