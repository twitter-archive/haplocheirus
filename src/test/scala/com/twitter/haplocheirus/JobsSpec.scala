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
      val append = Jobs.Append(data, List("t1", "t2"))
      val text = "{\"entry\":\"aGVsbG8=\\r\\n\",\"timelines\":[\"t1\",\"t2\"]}"

      expect {
        // yes, these 2 keys map to similar longs, but the byte_swapper in gizzard will fix that.
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
        one(shard1).append(data, "t1")
        one(shard2).append(data, "t2")
      }

      append.toMap mustEqual Map("entry" -> "aGVsbG8=\r\n", "timelines" -> List("t1", "t2"))
      append.apply(nameServer)

      Json.build(append.toMap).toString mustEqual text
      // can't compare byte arrays in case classes. suck.
      new Jobs.Append(Json.parse(text).asInstanceOf[Map[String, Any]]).timelines mustEqual append.timelines
    }

    "Remove" in {
      val data = "hello".getBytes
      val remove = Jobs.Remove(data, List("t1", "t2"))
      val text = "{\"entry\":\"aGVsbG8=\\r\\n\",\"timelines\":[\"t1\",\"t2\"]}"

      expect {
        // yes, these 2 keys map to similar longs, but the byte_swapper in gizzard will fix that.
        one(nameServer).findCurrentForwarding(0, 632754681242344982L) willReturn shard1
        one(nameServer).findCurrentForwarding(0, 632753581730716771L) willReturn shard2
        one(shard1).remove(data, "t1")
        one(shard2).remove(data, "t2")
      }

      remove.toMap mustEqual Map("entry" -> "aGVsbG8=\r\n", "timelines" -> List("t1", "t2"))
      remove.apply(nameServer)

      Json.build(remove.toMap).toString mustEqual text
      // can't compare byte arrays in case classes. suck.
      new Jobs.Remove(Json.parse(text).asInstanceOf[Map[String, Any]]).timelines mustEqual remove.timelines
    }
  }
}
