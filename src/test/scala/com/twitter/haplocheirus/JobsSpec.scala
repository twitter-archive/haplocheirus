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
      val append = Jobs.Append(data, "t1")
      val text = "{\"entry\":\"aGVsbG8=\",\"timeline\":\"t1\"}"

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).append(data, "t1", None)
      }

      append.toMap mustEqual Map("entry" -> "aGVsbG8=", "timeline" -> "t1")
      append.apply(nameServer)

      Json.build(append.toMap).toString mustEqual text
      // can't compare byte arrays in case classes. suck.
      new Jobs.Append(Json.parse(text).asInstanceOf[Map[String, Any]]).timeline mustEqual append.timeline
    }

    "Remove" in {
      val data = "hello".getBytes
      val remove = Jobs.Remove(data, "t1")
      val text = "{\"entry\":\"aGVsbG8=\",\"timeline\":\"t1\"}"

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).remove(data, "t1", None)
      }

      remove.toMap mustEqual Map("entry" -> "aGVsbG8=", "timeline" -> "t1")
      remove.apply(nameServer)

      Json.build(remove.toMap).toString mustEqual text
      // can't compare byte arrays in case classes. suck.
      new Jobs.Remove(Json.parse(text).asInstanceOf[Map[String, Any]]).timeline mustEqual remove.timeline
    }

    "Merge" in {
      val data = List("coke".getBytes, "zero".getBytes)
      val merge = Jobs.Merge("t1", data)
      val text = "{\"timeline\":\"t1\",\"entries\":[\"Y29rZQ==\",\"emVybw==\"]}"

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).merge("t1", data)
      }

      merge.toMap mustEqual Map("timeline" -> "t1", "entries" -> List("Y29rZQ==", "emVybw=="))
      merge.apply(nameServer)

      Json.build(merge.toMap).toString mustEqual text
      // can't compare byte arrays in case classes. suck.
      new Jobs.Merge(Json.parse(text).asInstanceOf[Map[String, Any]]).timeline mustEqual merge.timeline
    }

    "DeleteTimeline" in {
      val deleteTimeline = Jobs.DeleteTimeline("t1")
      val text = "{\"timeline\":\"t1\"}"

      expect {
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).deleteTimeline("t1")
      }

      deleteTimeline.toMap mustEqual Map("timeline" -> "t1")
      deleteTimeline.apply(nameServer)

      Json.build(deleteTimeline.toMap).toString mustEqual text
      new Jobs.DeleteTimeline(Json.parse(text).asInstanceOf[Map[String, Any]]) mustEqual deleteTimeline
    }
  }
}
