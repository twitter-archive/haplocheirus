package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JobScheduler, JsonJob, PrioritizingJobScheduler}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import thrift.conversions.TimelineSegment._


object TimelineStoreServiceSpec extends Specification with JMocker with ClassMocker {
  "TimelineStoreService" should {
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[PrioritizingJobScheduler[JsonJob]]
    val jobScheduler = mock[JobScheduler[JsonJob]]
    val multiPushScheduler = mock[JobScheduler[jobs.MultiPush]]
    val queue = mock[JobQueue[JsonJob]]
    val readPool = mock[RedisPool]
    val writePool = mock[RedisPool]
    val slowPool = mock[RedisPool]
    val shard1 = mock[HaplocheirusShard]
    val shard2 = mock[HaplocheirusShard]
    val copyFactory = mock[jobs.RedisCopyFactory]
    var service: TimelineStoreService = null

    doBefore {
      expect {
        one(scheduler).apply(Priority.Write.id) willReturn jobScheduler
        one(jobScheduler).queue willReturn queue
      }
      service = new TimelineStoreService(nameServer, scheduler, multiPushScheduler, copyFactory, readPool, writePool, slowPool)
      service.addOnError = false
    }

    "append" in {
      val data = "hello".getBytes
      val timelines = List("t1", "t2")

      expect {
        one(scheduler).apply(Priority.Write.id) willReturn jobScheduler
        one(multiPushScheduler).queue willReturn queue
        one(queue).put(any)
      }

      service.append(data, "t", List(1L, 2L))
    }

    "remove" in {
      val data = "hello".getBytes
      val timelines = List("t1", "t2")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
        one(shard1).remove("t1", List(data), None)
        one(shard2).remove("t2", List(data), None)
      }

      service.remove(data, "t", List(1L, 2L))
    }

    "filter" in {
      val data = "hello".getBytes
      val timeline = "t1"
      val entries = List(new Array[Byte](0))

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).oldFilter(timeline, entries, -1) willReturn Some(List(data))
      }

      service.filter(timeline, entries, -1) mustEqual Some(List(data))
    }

    "filter2" in {
      val data = "hello".getBytes
      val timeline = "t1"

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).filter(timeline, List(23L), -1) willReturn Some(List(data))
      }

      service.filter2(timeline, List(23L), -1) mustEqual Some(List(data))
    }

    "get" in {
      val offset = 10
      val length = 5
      val data = List("a".getBytes, "z".getBytes)

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).get("t1", offset, length, false) willReturn Some(TimelineSegment(data, 3))
      }

      service.get("t1", offset, length, false) mustEqual Some(TimelineSegment(data, 3))
    }

    "getRange" in {
      val fromId = 10L
      val toId = 7L
      val data = List("a".getBytes, "z".getBytes)

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).getRange("t1", fromId, toId, false) willReturn Some(TimelineSegment(data, 3))
      }

      service.getRange("t1", fromId, toId, false) mustEqual Some(TimelineSegment(data, 3))
    }

    "store" in {
      val data = List("a".getBytes, "z".getBytes)

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).store("t1", data)
      }

      service.store("t1", data)
    }

    "merge" in {
      val data = List("a".getBytes, "z".getBytes)
      val timeline = "t1"

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).merge("t1", data, None)
      }

      service.merge(timeline, data)
    }

    "unmerge" in {
      val data = List("a".getBytes, "z".getBytes)
      val timeline = "t1"

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).remove("t1", data, None)
      }

      service.unmerge(timeline, data)
    }

    "mergeIndirect" in {
      val data = List("a".getBytes, "z".getBytes)
      val timeline1 = "t1"
      val timeline2 = "t2"

      "with a source timeline" in {
        expect {
          one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
          one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
          one(shard2).getRaw("t2") willReturn Some(data)
          one(shard1).merge("t1", data, None)
        }

        service.mergeIndirect(timeline1, timeline2) mustEqual true
      }

      "without a source timeline" in {
        expect {
          one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
          one(shard2).getRaw("t2") willReturn None
        }

        service.mergeIndirect(timeline1, timeline2) mustEqual false
      }
    }

    "unmergeIndirect" in {
      val data = List("a".getBytes, "z".getBytes)
      val timeline1 = "t1"
      val timeline2 = "t2"

      "with a source timeline" in {
        expect {
          one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
          one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
          one(shard2).getRaw("t2") willReturn Some(data)
          one(shard1).remove("t1", data, None)
        }

        service.unmergeIndirect(timeline1, timeline2) mustEqual true
      }

      "without a source timeline" in {
        expect {
          one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
          one(shard2).getRaw("t2") willReturn None
        }

        service.unmergeIndirect(timeline1, timeline2) mustEqual false
      }
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
        one(multiPushScheduler).shutdown()
        one(readPool).shutdown()
        one(writePool).shutdown()
        one(slowPool).shutdown()
      }

      service.shutdown()
    }
  }
}
