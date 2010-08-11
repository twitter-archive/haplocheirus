package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.shards.{Busy, ShardId, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config, Configgy}
import org.jredis.ClientRuntimeException
import org.jredis.ri.alphazero.{JRedisClient, JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class RedisCopySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisCopy" should {
    val entries = List("1".getBytes, "2".getBytes)
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[JobScheduler]
    val shard1 = mock[HaplocheirusShard]
    val shard2 = mock[HaplocheirusShard]
    val shard1Id = ShardId("test", "shard1")
    val shard2Id = ShardId("test", "shard2")

    "start" in {
      val job = RedisCopyFactory(shard1Id, shard2Id)

      expect {
        one(nameServer).markShardBusy(shard2Id, Busy.Busy)
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).getKeys(RedisCopy.START, RedisCopy.COPY_COUNT) willReturn List("t1", "t2")
        one(shard2).startCopy("t1")
        one(shard1).getRaw("t1") willReturn entries
        one(shard2).doCopy("t1", entries)
        one(shard2).startCopy("t2")
        one(shard1).getRaw("t2") willReturn entries
        one(shard2).doCopy("t2", entries)
        one(scheduler).apply(new RedisCopy(shard1Id, shard2Id, 2, RedisCopy.COPY_COUNT))
      }

      job.apply((nameServer, scheduler))
    }

    "finish" in {
      val job = new RedisCopy(shard1Id, shard2Id, 2, RedisCopy.COPY_COUNT)

      expect {
        one(nameServer).markShardBusy(shard2Id, Busy.Busy)
        one(nameServer).findShardById(shard1Id) willReturn shard1
        one(nameServer).findShardById(shard2Id) willReturn shard2
        one(shard1).getKeys(2, RedisCopy.COPY_COUNT) willReturn List[String]()
        one(nameServer).markShardBusy(shard2Id, Busy.Normal)
      }

      job.apply((nameServer, scheduler))
    }
  }
}
