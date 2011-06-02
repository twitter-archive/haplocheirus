package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JobScheduler, JsonCodec, JsonJob, PrioritizingJobScheduler}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object JobsSpec extends Specification with JMocker with ClassMocker {
  "Jobs" should {
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[JobScheduler[JsonJob]]
    val shard1 = mock[HaplocheirusShard]
    val shard2 = mock[HaplocheirusShard]

    "Append" in {
      val data = "hello".getBytes
      val append = jobs.Append(data, "t1", nameServer)
      val map = Map("entry" -> "aGVsbG8=", "timeline" -> "t1")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).append("t1", List(data), None)
      }

      new jobs.AppendParser(null, nameServer)(map).toString mustEqual append.toString
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

      new jobs.RemoveParser(null, nameServer)(map).toString mustEqual remove.toString
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

      new jobs.MergeParser(null, nameServer)(map).toString mustEqual merge.toString
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

      new jobs.DeleteTimelineParser(null, nameServer)(map).toString mustEqual deleteTimeline.toString
      deleteTimeline.toMap mustEqual map
      deleteTimeline.apply()
    }

    "MultiPush" in {
      val data = "hello".getBytes
      val multiPush = jobs.MultiPush(data, "timeline:", Array(3L, 4L, 5L), nameServer, scheduler)

      "json" in {
        val map = Map("entry" -> "aGVsbG8=", "timeline_prefix" -> "timeline:",
                      "timeline_ids" -> "AwAAAAAAAAAEAAAAAAAAAAUAAAAAAAAA")

        multiPush.addOnError = false
        val queue = mock[JobQueue[JsonJob]]

        expect {
          allowing(scheduler).errorQueue willReturn queue
          allowing(scheduler).errorLimit willReturn 10
          one(nameServer).findCurrentForwarding(0, 776251139709896853L) willReturn shard1
          one(shard1).append("timeline:3", List(data), None)
          one(nameServer).findCurrentForwarding(0, 776243443128499376L) willReturn shard1
          one(shard1).append("timeline:4", List(data), None)
          one(nameServer).findCurrentForwarding(0, 776244542640127587L) willReturn shard1
          one(shard1).append("timeline:5", List(data), None)
        }

        val parser = new jobs.MultiPushParser(nameServer, scheduler)
        parser(map).entry.toList mustEqual multiPush.entry.toList
        parser(map).timelinePrefix mustEqual multiPush.timelinePrefix
        parser(map).timelineIds.toList mustEqual multiPush.timelineIds.toList
        multiPush.toMap mustEqual map
        multiPush.apply()
      }

      "thrift" in {
        val encoded =
          List[Byte](11, 0, 1, 0, 0, 0, 5, 104, 101, 108, 108, 111, 11, 0, 2, 0, 0, 0, 9, 116, 105,
               109, 101, 108, 105, 110, 101, 58, 15, 0, 3, 10, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3,
               0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0)

        val codec = new jobs.MultiPushCodec(nameServer, scheduler)
        codec.flatten(multiPush).toList mustEqual encoded
        val altPush = codec.inflate(encoded.toArray)
        altPush.entry.toList mustEqual multiPush.entry.toList
        altPush.timelinePrefix mustEqual multiPush.timelinePrefix
        altPush.timelineIds.toList mustEqual multiPush.timelineIds.toList
      }
    }
  }
}
