package com.twitter.haplocheirus

import java.util.{List => JList, Timer}
import java.util.concurrent.{Future, TimeUnit}
import com.twitter.gizzard.shards.{ShardException, ShardInfo}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.TimeConversions._
import org.jredis.ClientRuntimeException
import org.jredis.ri.alphazero.{JRedisClient, JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RedisShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisShard" should {
    val shardInfo = mock[ShardInfo]
    val poolHealthTracker = new RedisPoolHealthTracker(config.redisConfig.poolHealthTrackerConfig)
    var readPool: RedisPool = null
    var writePool: RedisPool = null
    var reads = 0
    var writes = 0
    var redisShard: HaplocheirusShard = null
    val jredis = mock[JRedisPipeline]
    val future = mock[Future[JList[Array[Byte]]]]
    val future2 = mock[Future[JList[Array[Byte]]]]
    val longFuture = mock[JRedisFutureSupport.FutureLong]
    val keysFuture = mock[Future[JList[String]]]
    val timelineTrimConfig =  new TimelineTrimConfig {
      val bounds = Map(
        "user_timeline" -> new TimelineTrimBounds { val lower = 3200; val upper = 3300 },
        "default" -> new TimelineTrimBounds { val lower = 800; val upper = 850 }
      )
    }
    val data = "hello".getBytes
    val timeline = "t1"

    val entry23share = TimelineEntry(24L, 23L, TimelineEntry.FLAG_SECONDARY_KEY).data
    val entry23 = TimelineEntry(23L, 0L, 0).data
    val entry23a = TimelineEntry(23L, 1L, TimelineEntry.FLAG_SECONDARY_KEY).data
    val entry22 = TimelineEntry(22L, 0L, 0).data
    val entry21 = TimelineEntry(21L, 0L, 0).data
    val entry20 = TimelineEntry(20L, 0L, TimelineEntry.FLAG_SECONDARY_KEY).data
    val entry19 = TimelineEntry(19L, 0L, 0).data
    val entry19a = TimelineEntry(19L, 1L, TimelineEntry.FLAG_SECONDARY_KEY).data
    val entry19uniq = TimelineEntry(19L, 1L, 0).data
    val entry17 = TimelineEntry(17L, 0L, 0).data
    val entry13 = TimelineEntry(13L, 0L, 0).data
    val entry10 = TimelineEntry(10L, 0L, 0).data

    def lrange(timeline: String, start: Int, end: Int, result: Seq[Array[Byte]]) {
      one(jredis).lrange(timeline, start, end) willReturn future
      one(future).get(1000, TimeUnit.MILLISECONDS) willReturn (Seq(TimelineEntry.EmptySentinel) ++ result).toJavaList
    }

    def lrangeWithoutEmptySentinel(timeline: String, start: Int, end: Int, result: Seq[Array[Byte]]) {
      one(jredis).lrange(timeline, start, end) willReturn future
      one(future).get(1000, TimeUnit.MILLISECONDS) willReturn result.toJavaList

    }

    def llen(timeline: String, result: Long) {
      one(jredis).llen(timeline) willReturn longFuture
      one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn result
    }

    var client: PipelinedRedisClient = null
    val timer = new Timer
    doBefore {
      PipelinedRedisClient.mockedOutJRedisClient = Some(jredis)
      client = new PipelinedRedisClient("", 10, 10, 100.milliseconds, 1.second, 1.second, 1.second, timer, { client: PipelinedRedisClient => }) {
        override protected def uniqueTimelineName(name: String): String = "generated-name"
      }
      reads = 0
      readPool = new RedisPool("read", poolHealthTracker, config.redisConfig.readPoolConfig) {
        override def withClient[T](shardInfo: ShardInfo)(f: PipelinedRedisClient => T): T = {
          reads += 1
          shardInfo.hostname
          f(client)
        }
      }
      writes = 0
      writePool = new RedisPool("write", poolHealthTracker, config.redisConfig.writePoolConfig) {
        override def withClient[T](shardInfo: ShardInfo)(f: PipelinedRedisClient => T): T = {
          writes += 1
          shardInfo.hostname
          f(client)
        }
      }
      redisShard = new RedisShardFactory(readPool, writePool, writePool, 3, timelineTrimConfig).instantiate(shardInfo, 1, Nil)
    }

    doAfter {
      PipelinedRedisClient.mockedOutJRedisClient = None
    }

    def operationCount = client.pipeline.operationCount

    "append" in {
      "doesn't need trim" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).rpushx(timeline, Array(data): _*) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 100L
          one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 100L
        }

        val oldOperationCount = operationCount
        redisShard.append(timeline, List(data), None)
        operationCount must eventually(be_==(oldOperationCount + 1))
      }

      "does need trim" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).rpushx(timeline, Array(data): _*) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 899L
          one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 899L
          one(jredis).ltrim(timeline, -800, -1)
        }

        val oldOperationCount = operationCount
        redisShard.append(timeline, List(data), None)
        operationCount must eventually(be_==(oldOperationCount + 1))
      }
    }

    "remove" in {
/*
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).lrem(timeline, data, 0)
      }

      redisShard.remove(timeline, List(data), None)
      writes mustEqual 1
*/
      expect {
        one(shardInfo).hostname willReturn "host1"
        lrange(timeline, 0, -1, List(entry23a, entry19a).reverse)
        one(jredis).lrem(timeline, entry23a, 0)
        one(jredis).expire(timeline, 1)
      }

      val oldOperationCount = operationCount
      redisShard.remove(timeline, List(entry23), None)
      operationCount must eventually(be_==(oldOperationCount + 1))
    }

    "filter" in {
      val entry1 = List(20L).pack
      val entry2 = List(21L).pack
      val entry3 = List(22L).pack
      val entry4 = List(23L).pack
      val entry5 = List(24L).pack
      val future = mock[Future[JList[Array[Byte]]]]

      "with no limit" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, 0, -1, List(entry20, entry22).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.filter(timeline, List(20L, 21L), -1).get.toList mustEqual List(entry20)
      }

      "with a search limit" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, -3, -1, List(entry21, entry22, entry23).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.filter(timeline, List(20L, 21L), 3).get.toList mustEqual List(entry21)
      }
    }

    "get" in {
      "unique entries" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, 0, -1, List(entry23, entry20, entry19).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, false).get.entries.toList mustEqual List(entry23, entry20, entry19)
        reads mustEqual 1
      }

      "limit and offset" in {
        "in range" in {
          expect {
            one(shardInfo).hostname willReturn "host1"
            lrange(timeline, 0, -1, List(entry23, entry20, entry19).reverse)
            one(jredis).expire(timeline, 1)
          }

          redisShard.get(timeline, 1, 10, false).get.entries.toList mustEqual List(entry20, entry19)
          reads mustEqual 1
        }

        "out of range" in {
          expect {
            one(shardInfo).hostname willReturn "host1"
            lrange(timeline, 0, -1, List(entry23))
            one(jredis).expire(timeline, 1)
          }

          redisShard.get(timeline, 10, 10, false).get.entries.toList mustEqual List()
          reads mustEqual 1
        }

        "miss" in {
          expect {
            one(shardInfo).hostname willReturn "host1"
            lrangeWithoutEmptySentinel(timeline, 0, -1, List())
          }

          redisShard.get(timeline, 0, 10, false) mustEqual None
          reads mustEqual 1
        }
      }

      "with duplicates in the sort key" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, 0, -1, List(entry23, entry23a, entry19).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, false).get.entries.toList mustEqual List(entry23, entry19)
        reads mustEqual 1
      }

      "with duplicates in the secondary key" in {
        "to be deduped" in {
          expect {
            allowing(shardInfo).hostname willReturn "host1"
            lrange(timeline, 0, -1, List(entry23a, entry20, entry19a).reverse)
            one(jredis).expire(timeline, 1)
            lrange(timeline, 0, -1, List(entry23a, entry20, entry19a).reverse)
            one(jredis).expire(timeline, 1)
          }

          redisShard.get(timeline, 0, 10, false).get.entries.toList mustEqual List(entry23a, entry20, entry19a)
          redisShard.get(timeline, 0, 10, true).get.entries.toList mustEqual List(entry20, entry19a)
          reads mustEqual 2
        }

        "not marked as having a secondary key" in {
          expect {
            allowing(shardInfo).hostname willReturn "host1"
            lrange(timeline, 0, -1, List(entry23a, entry20, entry19uniq).reverse)
            one(jredis).expire(timeline, 1)
          }

          redisShard.get(timeline, 0, 10, true).get.entries.toList mustEqual List(entry23a, entry20, entry19uniq)
          reads mustEqual 1
        }
      }

      "with duplicates between the secondary and primary keys" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, 0, -1, List(entry23share, entry23, entry20).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, true).get.entries.toList mustEqual List(entry23, entry20)
        reads mustEqual 1
      }

      "doesn't spaz when there aren't enough bytes to uniqify" in {
        val entry1 = "a".getBytes
        val entry2 = "lots-o-bytes".getBytes
        val entry3 = "almost!".getBytes

        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, 0, -1, List(entry1, entry2, entry3).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, true).get.entries.toList mustEqual List(entry2, entry3, entry1)
        reads mustEqual 1
      }
    }

    "getRaw" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        lrange(timeline, 0, -1, List(entry23, entry20, entry19).reverse)
        one(jredis).expire(timeline, 1)
      }

      redisShard.getRaw(timeline) mustEqual Some(List(entry23, entry20, entry19))
      reads mustEqual 1
    }

    "getRange" in {
      "with missing fromId" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, -600, -1, List(entry23, entry20, entry19, entry10).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.getRange(timeline, 11L, 0L, false).get.entries.toList mustEqual List(entry23, entry20, entry19)
        reads mustEqual 1
      }

      "with fromId" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, -600, -1, List(entry23, entry20, entry19, entry10).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.getRange(timeline, 19L, 0L, false).get.entries.toList mustEqual List(entry23, entry20)
        reads mustEqual 1
      }

      "with toId" in {
        expect {
          allowing(shardInfo).hostname willReturn "host1"
          lrange(timeline, -600, -1, List(entry23, entry20, entry19).reverse)
          one(jredis).expire(timeline, 1)
          lrange(timeline, -600, -1, List(entry23, entry20, entry19).reverse)
          one(jredis).expire(timeline, 1)
          lrange(timeline, -600, -1, List(entry23, entry20, entry19).reverse)
          one(jredis).expire(timeline, 1)
          allowing(jredis).expire(timeline, 1)
        }

        redisShard.getRange(timeline, 19L, 30L, false).get.entries.toList mustEqual List(entry23, entry20)
        redisShard.getRange(timeline, 19L, 23L, false).get.entries.toList mustEqual List(entry20)
        redisShard.getRange(timeline, 19L, 20L, false).get.entries.toList mustEqual List()
        reads mustEqual 3
      }

      "with dupes" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          lrange(timeline, -600, -1, List(entry23, entry20, entry20,entry20, entry17, entry13).reverse)
          one(jredis).expire(timeline, 1)
        }

        redisShard.getRange(timeline, 13L, 0L, false).get.entries.toList mustEqual List(entry23, entry20, entry17)
        reads mustEqual 1
      }

      "miss" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, -600, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn (Seq[TimelineEntry]()).toJavaList
        }

        redisShard.getRange(timeline, 19L, 20L, false) mustEqual None
        reads mustEqual 1
      }
    }

    "merge" in {
      val existing = List(20L, 18L, 16L, 12L, 7L)

      "no existing timeline" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List[Array[Byte]]().toJavaList
        }
        redisShard.merge(timeline, List(List(21L).pack.array), None)
        client.pipeline.size must eventually(be_==(0))
        writes mustEqual 1
      }

      "tiny little entries" in {
        val insert = List("a".getBytes, "b".getBytes)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack.array }.reverse.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).linsertBefore(timeline, List(7L).pack.array, insert(0)) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(jredis).linsertBefore(timeline, insert(0), insert(1))
        }

        val oldOperationCount = operationCount
        redisShard.merge(timeline, insert, None)
        operationCount must eventually(be_==(oldOperationCount + 2))
      }

      "nothing to merge" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack.array }.reverse.toJavaList
          one(jredis).expire(timeline, 1)
        }

        redisShard.merge(timeline, List[Array[Byte]](), None)
        client.pipeline.size must eventually(be_==(0))
      }

      "all prefix" in {
        val insert = List(29L, 28L, 21L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack.array }.reverse.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).rpushx(timeline, Array(List(29L).pack.array): _*) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(jredis).rpushx(timeline, Array(List(28L).pack.array): _*) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(jredis).rpushx(timeline, Array(List(21L).pack.array): _*)
        }

        val oldOperationCount = operationCount
        redisShard.merge(timeline, insert.map { List(_).pack.array }, None)
        operationCount must eventually(be_==(oldOperationCount + 3))
      }

      "all postfix" in {
        val insert = List(5L, 2L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack.array }.reverse.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).linsertBefore(timeline, List(7L).pack.array, List(5L).pack.array) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(jredis).linsertBefore(timeline, List(5L).pack.array, List(2L).pack.array)
        }

        val oldOperationCount = operationCount
        redisShard.merge(timeline, insert.map { List(_).pack.array }, None)
        operationCount must eventually(be_==(oldOperationCount + 2))
      }

      "all infix" in {
        val insert = List(19L, 14L, 13L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack.array }.reverse.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).linsertBefore(timeline, List(20L).pack.array, List(19L).pack.array) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(jredis).linsertBefore(timeline, List(16L).pack.array, List(14L).pack.array) willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(longFuture).get(1000, TimeUnit.MILLISECONDS)
          one(jredis).linsertBefore(timeline, List(14L).pack.array, List(13L).pack.array)
        }

        val oldOperationCount = operationCount
        redisShard.merge(timeline, insert.map { List(_).pack.array }, None)
        operationCount must eventually(be_==(oldOperationCount + 3))
      }

      "dupes" in {
        val insert = List(16L, 15L, 15L, 12L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack.array }.reverse.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).linsertBefore(timeline, List(16L).pack.array, List(15L).pack.array)
        }

        val oldOperationCount = operationCount
        redisShard.merge(timeline, insert.map { List(_).pack.array }, None)
        operationCount must eventually(be_==(oldOperationCount + 1))
      }
    }

    "store" in {
      val entry1 = List(23L).pack.array
      val entry2 = List(20L).pack.array
      val entry3 = List(19L).pack.array

      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).rpush("generated-name", TimelineEntry.EmptySentinel)
        one(jredis).rpushx("generated-name", entry3, entry2, entry1)
        one(jredis).rename("generated-name", timeline)
        one(jredis).expire(timeline, 1)
      }

      redisShard.store(timeline, List(entry1, entry2, entry3))
      writes mustEqual 1
    }

    "deleteTimeline" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).del(timeline) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 0L
      }

      redisShard.deleteTimeline(timeline)
      writes mustEqual 1
    }

    "getKeys" in {
      "from start" in {
        expect {
          one(shardInfo).hostname willReturn "host1"

          one(jredis).keys() willReturn keysFuture
          one(keysFuture).get(1000, TimeUnit.MILLISECONDS) willReturn List("a", "b", "c").map(_.getBytes).toJavaList
          one(jredis).ltrim("%keys", 1, 0)
          one(jredis).rpush("%keys", "a".getBytes)
          one(jredis).rpush("%keys", "b".getBytes)
          one(jredis).rpush("%keys", "c".getBytes)
          one(jredis).llen("%keys") willReturn longFuture
          one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 4L

          one(jredis).lrange("%keys", 0, 1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List("a", "b").map { _.getBytes }.toJavaList
        }

        redisShard.getKeys(0, 2).toList mustEqual List("a", "b")
        reads mustEqual 1
      }

      "from middle" in {
        expect {
          one(shardInfo).hostname willReturn "host1"

          one(jredis).lrange("%keys", 2, 3) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List("c").map { _.getBytes }.toJavaList
        }

        redisShard.getKeys(2, 2).toList mustEqual List("c")
        reads mustEqual 1
      }

      "at the end" in {
        expect {
          one(shardInfo).hostname willReturn "host1"

          one(jredis).lrange("%keys", 4, 5) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List[Array[Byte]]().toJavaList
        }

        redisShard.getKeys(4, 2).toList mustEqual List[String]()
        reads mustEqual 1
      }
    }

    "startCopy" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).del(timeline)
        one(jredis).rpush(timeline, TimelineEntry.EmptySentinel)
      }

      redisShard.startCopy(timeline)
      writes mustEqual 1
    }

    "doCopy" in {
      val entry1 = List(23L).pack.array
      val entry2 = List(20L).pack.array

      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).lpushx(timeline, entry1, entry2)
        one(jredis).expire(timeline, 1) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn 1L
      }

      redisShard.doCopy(timeline, List(entry1, entry2))
      writes mustEqual 1
    }

    "exceptions are wrapped" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).lrange(timeline, 0, -1) willThrow new IllegalStateException("aiee")
      }

      redisShard.get(timeline, 0, 10, true) must throwA[ShardException]
    }
  }
}
