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
        // yes, these 2 keys map to similar longs, but the byte_swapper in gizzard will fix that.
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
        // yes, these 2 keys map to similar longs, but the byte_swapper in gizzard will fix that.
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(shard1).remove(data, "t1", None)
      }

      remove.toMap mustEqual Map("entry" -> "aGVsbG8=", "timeline" -> "t1")
      remove.apply(nameServer)

      Json.build(remove.toMap).toString mustEqual text
      // can't compare byte arrays in case classes. suck.
      new Jobs.Remove(Json.parse(text).asInstanceOf[Map[String, Any]]).timeline mustEqual remove.timeline
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
