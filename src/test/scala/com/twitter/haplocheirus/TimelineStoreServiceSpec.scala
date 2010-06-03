package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{ErrorHandlingJobQueue, JobScheduler, PrioritizingJobScheduler}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object TimelineStoreServiceSpec extends Specification with JMocker with ClassMocker {
  "TimelineStoreService" should {
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[PrioritizingJobScheduler]
    val jobScheduler = mock[JobScheduler]
    val queue = mock[ErrorHandlingJobQueue]
    val redisPool = mock[RedisPool]
    val future = mock[Future]
    val replicationFuture = mock[Future]
    val shard1 = mock[HaplocheirusShard]
    val shard2 = mock[HaplocheirusShard]
    var service: TimelineStoreService = null

    doBefore {
      expect {
        one(scheduler).apply(Priority.Write.id) willReturn jobScheduler
        one(jobScheduler).queue willReturn queue
      }
      service = new TimelineStoreService(nameServer, scheduler, Jobs.RedisCopyFactory, redisPool, future, replicationFuture)
      service.addOnError = false
    }

    "append" in {
      val data = "hello".getBytes
      val timelines = List("t1", "t2")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
        one(shard1).append(data, "t1", None)
        one(shard2).append(data, "t2", None)
      }

      service.append(data, timelines)
    }

    "remove" in {
      val data = "hello".getBytes
      val timelines = List("t1", "t2")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
        one(shard1).remove(data, "t1", None)
        one(shard2).remove(data, "t2", None)
      }

      service.remove(data, timelines)
    }

    "get" in {
      val offset = 10
      val length = 5
      val data = List("a".getBytes, "z".getBytes)

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).get("t1", offset, length, false) willReturn data
      }

      service.get("t1", offset, length, false) mustEqual data
    }

    "getSince" in {
      val fromId = 10L
      val data = List("a".getBytes, "z".getBytes)

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).getSince("t1", fromId, false) willReturn data
      }

      service.getSince("t1", fromId, false) mustEqual data
    }

    "deleteTimeline" in {
      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).deleteTimeline("t1")
      }

      service.deleteTimeline("t1")
    }

    "shutdown" in {
      expect {
        one(scheduler).shutdown()
        one(future).shutdown()
        one(replicationFuture).shutdown()
        one(redisPool).shutdown()
      }

      service.shutdown()
    }
  }
}
