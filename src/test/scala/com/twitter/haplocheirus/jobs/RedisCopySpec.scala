package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobScheduler, JsonJob}
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config, Configgy}
import org.jredis.ClientRuntimeException
import org.jredis.ri.alphazero.{JRedisClient, JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class RedisCopySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val shard1Id = ShardId("test", "shard1")
  val shard2Id = ShardId("test", "shard2")

  val shard2Info = new ShardInfo("", "", "")

  "RedisCopy" should {
    val entries = List("1".getBytes, "2".getBytes)
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[JobScheduler[JsonJob]]
    val shard1 = mock[HaplocheirusShard]
    val shard2 = mock[HaplocheirusShard]

    "start" in {
      val job = new RedisCopyFactory(nameServer, scheduler)(shard1Id, shard2Id)

      "normally" in {
        expect {
          one(nameServer).getShard(shard2Id) willReturn shard2Info
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).getKeys(RedisCopy.START, RedisCopy.COPY_COUNT) willReturn List("t1", "t2")
          one(shard2).startCopy("t1")
          one(shard1).getRaw("t1") willReturn Some(entries)
          one(shard2).doCopy("t1", entries)
          one(shard2).startCopy("t2")
          one(shard1).getRaw("t2") willReturn Some(entries)
          one(shard2).doCopy("t2", entries)
          one(scheduler).put(new RedisCopy(shard1Id, shard2Id, 2, RedisCopy.COPY_COUNT, nameServer, scheduler))
        }

        job.apply()
      }

      "with missing data" in {
        expect {
          one(nameServer).getShard(shard2Id) willReturn shard2Info
          one(nameServer).markShardBusy(shard2Id, Busy.Busy)
          one(nameServer).findShardById(shard1Id) willReturn shard1
          one(nameServer).findShardById(shard2Id) willReturn shard2
          one(shard1).getKeys(RedisCopy.START, RedisCopy.COPY_COUNT) willReturn List("t1", "t2")
          one(shard2).startCopy("t1")
          one(shard1).getRaw("t1") willReturn Some(entries)
          one(shard2).doCopy("t1", entries)
          one(shard2).startCopy("t2")
          one(shard1).getRaw("t2") willReturn None
          one(shard2).deleteTimeline("t2")
          one(scheduler).put(new RedisCopy(shard1Id, shard2Id, 2, RedisCopy.COPY_COUNT, nameServer, scheduler))
        }

        job.apply()
      }
    }

    "finish" in {
      val job = new RedisCopy(shard1Id, shard2Id, 2, RedisCopy.COPY_COUNT, nameServer, scheduler)

      expect {
        one(nameServer).getShard(shard2Id) willReturn shard2Info
        one(nameServer).markShardBusy(shard2Id, Busy.Busy)
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).getKeys(2, RedisCopy.COPY_COUNT) willReturn List[String]()
        one(nameServer).markShardBusy(shard2Id, Busy.Normal)
      }

      job.apply()
    }

    "toJson" in {
      val job = new RedisCopy(shard1Id, shard2Id, 500, 200, nameServer, scheduler)
      val json = job.toJson
      json mustMatch "Copy"
      json mustMatch "\"cursor\":" + 500
      json mustMatch "\"count\":" + 200
    }
  }

  "RedisCopyParser" should {
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[JobScheduler[JsonJob]]

    "parse" in {
      val parser = new RedisCopyParser(nameServer, scheduler)
      val json = Map("source_shard_table_prefix" -> "shard1",
                     "source_shard_hostname" -> "test",
                     "destination_shard_table_prefix" -> "shard2",
                     "destination_shard_hostname" -> "test",
                     "cursor" -> 500, "count" -> 200)
      parser(json) mustEqual new RedisCopy(shard1Id, shard2Id, 500, 200, nameServer, scheduler)
    }
  }
}
