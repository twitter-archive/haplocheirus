package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JobScheduler, JsonCodec, JsonJob, PrioritizingJobScheduler}
import com.twitter.json.{Json, JsonQuoted}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object JobsSpec extends Specification with JMocker with ClassMocker {
  "Jobs" should {
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[JobScheduler[JsonJob]]
    val shard1 = mock[HaplocheirusShard]
    val shard2 = mock[HaplocheirusShard]
    val codec = mock[JsonCodec[JsonJob]]

    "Append" in {
      val data = "hello".getBytes
      val append = jobs.Append(data, "t1", nameServer)
      val map = Map("entry" -> "aGVsbG8=", "timeline" -> "t1")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).append("t1", List(data), None)
      }

      new jobs.AppendParser(null, nameServer)(codec, map).toString mustEqual append.toString
      append.toMap mustEqual map
      append.apply()
    }

    "Remove" in {
      val data = List("coke".getBytes, "zero".getBytes)
      val remove = jobs.Remove("t1", data, nameServer)
      val map = Map("timeline" -> "t1", "entries" -> List("Y29rZQ==", "emVybw=="))

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).remove("t1", data, None)
      }

      new jobs.RemoveParser(null, nameServer)(codec, map).toString mustEqual remove.toString
      remove.toMap mustEqual map
      remove.apply()
    }

    "Merge" in {
      val data = List("coke".getBytes, "zero".getBytes)
      val merge = jobs.Merge("t1", data, nameServer)
      val map = Map("timeline" -> "t1", "entries" -> List("Y29rZQ==", "emVybw=="))

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).merge("t1", data, None)
      }

      new jobs.MergeParser(null, nameServer)(codec, map).toString mustEqual merge.toString
      merge.toMap mustEqual map
      merge.apply()
    }

    "DeleteTimeline" in {
      val deleteTimeline = jobs.DeleteTimeline("t1", nameServer)
      val map = Map("timeline" -> "t1")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).deleteTimeline("t1")
      }

      new jobs.DeleteTimelineParser(null, nameServer)(codec, map).toString mustEqual deleteTimeline.toString
      deleteTimeline.toMap mustEqual map
      deleteTimeline.apply()
    }

    "MultiPush" in {
      val data = "hello".getBytes
      val multiPush = jobs.MultiPush(data, "timeline:", List(3L, 4L, 5L), nameServer, scheduler)
      val map = Map("entry" -> JsonQuoted("\"aGVsbG8=\""), "timeline_prefix" -> "timeline:",
                    "timeline_ids" -> JsonQuoted("\"AwAAAAAAAAAEAAAAAAAAAAUAAAAAAAAA\""))

      multiPush.addOnError = false
      val queue = mock[JobQueue[JsonJob]]

      expect {
        allowing(scheduler).queue willReturn queue
        one(nameServer).findCurrentForwarding(0, 776251139709896853L) willReturn shard1
        one(shard1).append("timeline:3", List(data), None)
        one(nameServer).findCurrentForwarding(0, 776243443128499376L) willReturn shard1
        one(shard1).append("timeline:4", List(data), None)
        one(nameServer).findCurrentForwarding(0, 776244542640127587L) willReturn shard1
        one(shard1).append("timeline:5", List(data), None)
      }

      val parser = new jobs.MultiPushParser(nameServer, scheduler)
      parser(codec, map).entry.toList mustEqual multiPush.entry.toList
      parser(codec, map).timelinePrefix mustEqual multiPush.timelinePrefix
      parser(codec, map).timelineIds.toList mustEqual multiPush.timelineIds.toList
      multiPush.toMap mustEqual map
      multiPush.apply()
    }
  }
}
