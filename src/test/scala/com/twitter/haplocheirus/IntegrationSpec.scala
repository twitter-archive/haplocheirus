package com.twitter.haplocheirus

import java.util.concurrent.{ExecutionException, Future, TimeUnit, TimeoutException}
import java.util.{List => JList}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.scheduler.{JsonJob, MemoryJobQueue}
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object IntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "Haplocheirus" should {
    val jredisClient = mock[JRedisPipeline]
    val future = mock[JRedisFutureSupport.FutureLong]
    val timelineFuture = mock[Future[JList[Array[Byte]]]]
    var service: Haplocheirus = null

    def errorQueue = {
      service.jobScheduler(Priority.Write.id).errorQueue.asInstanceOf[MemoryJobQueue[JsonJob]]
    }

    val shardId11 = new ShardId("localhost", "dev11a")
    val shardId12 = new ShardId("localhost", "dev12a")
    val shardId2 = new ShardId("localhost", "dev1b")
    val shardIdR = new ShardId("localhost", "dev1")

    Stats.sampleRate = 1

    doBefore {
      PipelinedRedisClient.mockedOutJRedisClient = Some(jredisClient)
      service = new Haplocheirus(config)

      service.nameServer.createShard(new ShardInfo(shardId11, "com.twitter.haplocheirus.RedisShard", "", "", Busy.Normal))
      service.nameServer.createShard(new ShardInfo(shardId12, "com.twitter.haplocheirus.RedisShard", "", "", Busy.Normal))
      service.nameServer.createShard(new ShardInfo(shardId2, "com.twitter.haplocheirus.RedisShard", "", "", Busy.Normal))
      service.nameServer.createShard(new ShardInfo(shardIdR, "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal))
      service.nameServer.addLink(shardIdR, shardId11, 1)
      service.nameServer.addLink(shardIdR, shardId12, 1)
      service.nameServer.setForwarding(new Forwarding(0, 0, shardIdR))

      service.start()
    }

    doAfter {
      expect {
        one(jredisClient).quit()
        one(jredisClient).quit()
      }

      service.shutdown()
      PipelinedRedisClient.mockedOutJRedisClient = None
    }

    val timeline1 = "home_timeline:109"
    val timeline2 = "home_timeline:77777"
    val data = List(123L).pack

    def pushAttempts() = {
      Stats.getTiming("redis-push-usec").get(false).count
    }

    "perform a basic append" in {
      // tricksy: since the expectations are met in another thread, we have to manually assert
      // that they happened.
      expect {
        one(jredisClient).rpushx(timeline1, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(jredisClient).rpushx(timeline1, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 2L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 2L
        one(jredisClient).rpushx(timeline2, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(jredisClient).rpushx(timeline2, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 2L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 2L
      }

      val oldCount = pushAttempts()
      service.jobScheduler.size mustEqual 0
      service.haploService.append(data, "home_timeline:", List(109L, 77777L).toJavaList)
      pushAttempts() must eventually(be_==(oldCount + 4))
    }

    "write to the error log on failure, and retry successfully" in {
      expect {
        one(jredisClient).rpushx(timeline1, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(jredisClient).rpushx(timeline1, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(jredisClient).rpushx(timeline2, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 1L
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn 1L
        one(jredisClient).rpushx(timeline2, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willThrow new ExecutionException(new Exception("Oups!"))
      }

      val oldCount = pushAttempts()
      errorQueue.size mustEqual 0
      service.haploService.append(data, "home_timeline:", List(109L, 77777L).toJavaList)
      pushAttempts() must eventually(be_==(oldCount + 4))
      errorQueue.size must eventually(be_==(1))

      expect {
        allowing(jredisClient).rpushx(timeline2, Array(data.array): _*) willReturn future
        allowing(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 3L
        allowing(future).get(200L, TimeUnit.MILLISECONDS) willReturn 3L
      }

      service.jobScheduler.retryErrors()
      errorQueue.size mustEqual 0
    }

    "only call error handler once on multiple failure" in {
      expect {
        one(jredisClient).rpushx(timeline1, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willThrow new ExecutionException(new Exception("Oops!"))
        one(jredisClient).rpushx(timeline1, Array(data.array): _*) willReturn future
        one(future).get(1000L, TimeUnit.MILLISECONDS) willThrow new ExecutionException(new Exception("Oops!"))
      }

      val oldCount = pushAttempts()
      errorQueue.size mustEqual 0
      service.haploService.append(data, "home_timeline:", List(109L).toJavaList)
      pushAttempts() must eventually(be_==(oldCount + 2))
      errorQueue.size must eventually(be_==(1))

      expect {
        allowing(jredisClient).rpushx(timeline1, Array(data.array): _*) willReturn future
        allowing(future).get(1000L, TimeUnit.MILLISECONDS) willReturn 3L
        allowing(future).get(200L, TimeUnit.MILLISECONDS) willReturn 3L
      }

      service.jobScheduler.retryErrors()
      errorQueue.size mustEqual 0
    }

    "rebuild one shard from another" in {
      service.nameServer.addLink(shardIdR, shardId2, 1)
      service.nameServer.reload()

      expect {
        one(jredisClient).lrange(timeline1, 0, -1) willReturn timelineFuture
        one(timelineFuture).get(200L, TimeUnit.MILLISECONDS) willReturn  Seq[Array[Byte]]().toJavaList

        one(jredisClient).lrange(timeline1, 0, -1) willReturn timelineFuture
        one(timelineFuture).get(200L, TimeUnit.MILLISECONDS) willReturn List("a", "b").map { _.getBytes }.toJavaList
        one(jredisClient).expire(timeline1, 86400)

        one(jredisClient).lrange(timeline1, 0, -1) willReturn timelineFuture
        one(timelineFuture).get(200L, TimeUnit.MILLISECONDS) willReturn List("a", "b").map { _.getBytes }.toJavaList
        one(jredisClient).expire(timeline1, 86400)

        one(jredisClient).del(timeline1)
        one(jredisClient).rpush(timeline1, TimelineEntry.EmptySentinel)
        one(jredisClient).lpushx(timeline1, Array("b", "a").map(_.getBytes): _*)
        one(jredisClient).expire(timeline1, 86400)
        allowing(jredisClient).quit()
      }

      val segment = service.haploService.get(timeline1, 0, 2, false)
      segment.size mustEqual 2
      segment.entries.get(0).array.toList mustEqual "a".getBytes.toList
      segment.entries.get(1).array.toList mustEqual "b".getBytes.toList
    }

    "get_multi hit" in {
      expect {
        one(jredisClient).lrange(timeline1, 0, -1) willReturn timelineFuture
        one(timelineFuture).get(200L, TimeUnit.MILLISECONDS) willReturn List("a", "b").map { _.getBytes }.toJavaList
        one(jredisClient).expire(timeline1, 86400)
      }

      val cmd = new thrift.TimelineGet(timeline1, 0, 2)
      cmd.dedupe = false
      val results = service.haploService.get_multi(Seq(cmd).toJavaList)
      results.size mustEqual 1
      val segment = results.toSeq(0)
      segment.state mustEqual thrift.TimelineSegmentState.HIT
      segment.size mustEqual 2
      segment.entries.get(0).array.toList mustEqual "a".getBytes.toList
      segment.entries.get(1).array.toList mustEqual "b".getBytes.toList
    }

    "get_multi miss" in {
      expect {
        one(jredisClient).lrange(timeline1, 0, -1) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn  Seq[Array[Byte]]().toJavaList
        one(jredisClient).lrange(timeline1, 0, -1) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn  Seq[Array[Byte]]().toJavaList
      }

      val cmd = new thrift.TimelineGet(timeline1, 0, 2)
      cmd.dedupe = false
      val results = service.haploService.get_multi(Seq(cmd).toJavaList)
      results.size mustEqual 1
      val segment = results.toSeq(0)
      segment.state mustEqual thrift.TimelineSegmentState.MISS
      segment.size mustEqual 0
    }

    "get_multi exception" in {
      expect {
        one(jredisClient).lrange(timeline1, 0, -1) willReturn future
        one(future).get(200L, TimeUnit.MILLISECONDS) willReturn Seq[Array[Byte]]().toJavaList
      }

      val cmd = new thrift.TimelineGet(timeline1, 0, 2)
      cmd.dedupe = false

      val oldTimeout = service.multiGetPool.timeout
      service.multiGetPool.timeout = 0.seconds

      val results = service.haploService.get_multi(Seq(cmd).toJavaList)

      service.multiGetPool.timeout = oldTimeout

      results.size mustEqual 1
      val segment = results.toSeq(0)
      segment.state mustEqual thrift.TimelineSegmentState.TIMEOUT
      segment.size mustEqual 0
    }
  }
}
