package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobScheduler, PrioritizingJobScheduler}
import com.twitter.json.Json
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object JobsSpec extends Specification with JMocker with ClassMocker {
  "Jobs" should {
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val shard1 = mock[HaplocheirusShard]
    val shard2 = mock[HaplocheirusShard]

    "Append" in {
      val data = "hello".getBytes
      val append = jobs.Append(data, "t1")
      val map = Map("entry" -> "aGVsbG8=", "timeline" -> "t1")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).append(data, "t1", None)
      }

      jobs.AppendParser(map).toString mustEqual append.toString
      append.toMap mustEqual map
      append.apply(nameServer)
    }

    "Remove" in {
      val data = "hello".getBytes
      val remove = jobs.Remove(data, "t1")
      val map = Map("entry" -> "aGVsbG8=", "timeline" -> "t1")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).remove(data, "t1", None)
      }

      jobs.RemoveParser(map).toString mustEqual remove.toString
      remove.toMap mustEqual map
      remove.apply(nameServer)
    }

    "Merge" in {
      val data = List("coke".getBytes, "zero".getBytes)
      val merge = jobs.Merge("t1", data)
      val map = Map("timeline" -> "t1", "entries" -> List("Y29rZQ==", "emVybw=="))

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).merge("t1", data, None)
      }

      jobs.MergeParser(map).toString mustEqual merge.toString
      merge.toMap mustEqual map
      merge.apply(nameServer)
    }

    "MergeIndirect" in {
      val data = List("coke".getBytes, "zero".getBytes)
      val mergeIndirect = jobs.MergeIndirect("t1", "t2")
      val map = Map("dest_timeline" -> "t1", "source_timeline" -> "t2")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
        one(shard2).getRaw("t2") willReturn Some(data)
        one(shard1).merge("t1", data, None)
      }

      jobs.MergeIndirectParser(map).toString mustEqual mergeIndirect.toString
      mergeIndirect.toMap mustEqual map
      mergeIndirect.apply(nameServer)
    }

    "UnmergeIndirect" in {
      val data = List("coke".getBytes, "zero".getBytes)
      val unmergeIndirect = jobs.UnmergeIndirect("t1", "t2")
      val map = Map("dest_timeline" -> "t1", "source_timeline" -> "t2")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
        one(shard2).getRaw("t2") willReturn Some(data)
        one(shard1).unmerge("t1", data, None)
      }

      jobs.UnmergeIndirectParser(map).toString mustEqual unmergeIndirect.toString
      unmergeIndirect.toMap mustEqual map
      unmergeIndirect.apply(nameServer)
    }

    "DeleteTimeline" in {
      val deleteTimeline = jobs.DeleteTimeline("t1")
      val map = Map("timeline" -> "t1")

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).deleteTimeline("t1")
      }

      jobs.DeleteTimelineParser(map).toString mustEqual deleteTimeline.toString
      deleteTimeline.toMap mustEqual map
      deleteTimeline.apply(nameServer)
    }
  }
}
