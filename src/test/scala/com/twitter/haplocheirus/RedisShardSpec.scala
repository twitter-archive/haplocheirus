package com.twitter.haplocheirus

import java.util.{List => JList}
import java.util.concurrent.{Future, TimeUnit}
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards.{ShardException, ShardInfo}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Configgy
import org.jredis.ClientRuntimeException
import org.jredis.ri.alphazero.{JRedisClient, JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RedisShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisShard" should {
    val shardInfo = mock[ShardInfo]
    var redisPool: RedisPool = null
    var redisShard: HaplocheirusShard = null
    val jredis = mock[JRedisPipeline]
    val future = mock[Future[JList[Array[Byte]]]]
    val future2 = mock[Future[JList[Array[Byte]]]]
    val longFuture = mock[Future[Long]]
    val config = Configgy.config.configMap("redis")
    val data = "hello".getBytes
    val timeline = "t1"

    doBefore {
      PipelinedRedisClient.mockedOutJRedisClient = Some(jredis)
      val client = new PipelinedRedisClient("", 0, 1.second, 1.second) {
        override protected def uniqueTimelineName(name: String): String = "generated-name"
      }
      redisPool = new RedisPool(config) {
        override def withClient[T](hostname: String)(f: PipelinedRedisClient => T): T = f(client)
      }
      redisShard = new RedisShardFactory(redisPool, 3).instantiate(shardInfo, 1, Nil)
    }

    doAfter {
      PipelinedRedisClient.mockedOutJRedisClient = None
    }

    "append" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).lpushx(timeline, data)
      }

      redisShard.append(data, timeline, None)
    }

    "remove" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).lrem(timeline, data, 0)
      }

      redisShard.remove(data, timeline, None)
    }

    "filter" in {
      val entry1 = List(20L).pack
      val entry2 = List(21L).pack
      val entry3 = List(22L).pack
      val future = mock[Future[JList[Array[Byte]]]]

      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).lrange(timeline, 0, -1) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry2, entry3).toJavaList
        one(jredis).expire(timeline, 1)
      }

      redisShard.filter(timeline, List(entry1, entry2)).toList mustEqual List(entry2)
    }

    "get" in {
      "unique entries" in {
        val entry1 = List(23L).pack
        val entry2 = List(20L).pack
        val entry3 = List(19L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, 9) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, false).toList mustEqual List(entry1, entry2, entry3)
      }

      "with duplicates in the sort key" in {
        val entry1 = List(23L, 1L).pack
        val entry2 = List(23L, 2L).pack
        val entry3 = List(19L, 3L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, 9) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, false).toList mustEqual List(entry2, entry3)
      }

      "with duplicates in the dedupe key" in {
        val entry1 = List(23L, 1L).pack
        val entry2 = List(21L, 2L).pack
        val entry3 = List(19L, 1L).pack

        expect {
          allowing(shardInfo).hostname willReturn "host1"
          allowing(jredis).lrange(timeline, 0, 9) willReturn future
          allowing(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
          allowing(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, false).toList mustEqual List(entry1, entry2, entry3)
        redisShard.get(timeline, 0, 10, true).toList mustEqual List(entry2, entry3)
      }

      "with missing dedupe key" in {
        val entry1 = List(23L, 1L).pack
        val entry2 = List(21L).pack
        val entry3 = List(19L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, 9) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, true).toList mustEqual List(entry1, entry2, entry3)
      }

      "doesn't spaz when there aren't enough bytes to uniqify" in {
        val entry1 = "a".getBytes
        val entry2 = "lots-o-bytes".getBytes
        val entry3 = "almost!".getBytes

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, 9) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
          one(jredis).expire(timeline, 1)
        }

        redisShard.get(timeline, 0, 10, true).toList mustEqual List(entry1, entry2, entry3)
      }
    }

    "getRange" in {
      "with missing fromId" in {
        val entry1 = List(23L).pack
        val entry2 = List(20L).pack
        val entry3 = List(19L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, 2) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).lrange(timeline, 3, 5) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List[Array[Byte]]().toJavaList
        }

        redisShard.getRange(timeline, 10L, 0L, false).toList mustEqual List(entry1, entry2, entry3)
      }

      "with fromId" in {
        "in the first page" in {
          val entry1 = List(23L).pack
          val entry2 = List(20L).pack
          val entry3 = List(19L).pack

          expect {
            one(shardInfo).hostname willReturn "host1"
            one(jredis).lrange(timeline, 0, 2) willReturn future
            one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
            one(jredis).expire(timeline, 1)
          }

          redisShard.getRange(timeline, 19L, 0L, false).toList mustEqual List(entry1, entry2)
        }

        "in a later page" in {
          val entry1 = List(23L).pack
          val entry2 = List(20L).pack
          val entry3 = List(19L).pack
          val entry4 = List(17L).pack
          val entry5 = List(13L).pack
          val entry6 = List(10L).pack

          expect {
            one(shardInfo).hostname willReturn "host1"
            one(jredis).lrange(timeline, 0, 2) willReturn future
            one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
            one(jredis).expire(timeline, 1)
            one(jredis).lrange(timeline, 3, 5) willReturn future
            one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry3, entry4, entry5).toJavaList
            one(jredis).expire(timeline, 1)
          }

          redisShard.getRange(timeline, 13L, 0L, false).toList mustEqual List(entry1, entry2, entry3, entry4)
        }
      }

      "with toId" in {
        "in the first page" in {
          val entry1 = List(23L).pack
          val entry2 = List(20L).pack
          val entry3 = List(19L).pack

          expect {
            allowing(shardInfo).hostname willReturn "host1"
            allowing(jredis).lrange(timeline, 0, 2) willReturn future
            allowing(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
            allowing(jredis).expire(timeline, 1)
          }

          redisShard.getRange(timeline, 19L, 30L, false).toList mustEqual List(entry1, entry2)
          redisShard.getRange(timeline, 19L, 23L, false).toList mustEqual List(entry1, entry2)
          redisShard.getRange(timeline, 19L, 20L, false).toList mustEqual List(entry2)
        }

        "in a later page" in {
          val entry1 = List(23L).pack
          val entry2 = List(20L).pack
          val entry3 = List(19L).pack
          val entry4 = List(17L).pack
          val entry5 = List(13L).pack
          val entry6 = List(10L).pack

          expect {
            allowing(shardInfo).hostname willReturn "host1"
            allowing(jredis).lrange(timeline, 0, 2) willReturn future
            allowing(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
            allowing(jredis).expire(timeline, 1)
            allowing(jredis).lrange(timeline, 3, 5) willReturn future2
            allowing(future2).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry3, entry4, entry5).toJavaList
          }

          redisShard.getRange(timeline, 13L, 30L, false).toList mustEqual List(entry1, entry2, entry3, entry4)
          redisShard.getRange(timeline, 13L, 20L, false).toList mustEqual List(entry2, entry3, entry4)
          redisShard.getRange(timeline, 13L, 17L, false).toList mustEqual List(entry4)
        }
      }

      "with dupes" in {
        val entry1 = List(23L).pack
        val entry2 = List(20L).pack
        val entry3 = List(20L).pack
        val entry4 = List(17L).pack
        val entry5 = List(13L).pack
        val entry6 = List(10L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, 2) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry1, entry2, entry3).toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).lrange(timeline, 3, 5) willReturn future2
          one(future2).get(1000, TimeUnit.MILLISECONDS) willReturn List(entry3, entry4, entry5).toJavaList
          one(jredis).expire(timeline, 1)
        }

        redisShard.getRange(timeline, 13L, 0L, false).toList mustEqual List(entry1, entry3, entry4)
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

        redisShard.merge(timeline, List(List(21L).pack), None)
      }

      "nothing to merge" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack }.toJavaList
          one(jredis).expire(timeline, 1)
        }

        redisShard.merge(timeline, List[Array[Byte]](), None)
      }

      "all prefix" in {
        val insert = List(29L, 28L, 21L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack }.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).lpushx(timeline, List(29L).pack)
          one(jredis).lpushx(timeline, List(28L).pack)
          one(jredis).lpushx(timeline, List(21L).pack)
        }

        redisShard.merge(timeline, insert.map { List(_).pack }, None)
      }

      "all postfix" in {
        val insert = List(5L, 2L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack }.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).linsertAfter(timeline, List(7L).pack, List(5L).pack)
          one(jredis).linsertAfter(timeline, List(5L).pack, List(2L).pack)
        }

        redisShard.merge(timeline, insert.map { List(_).pack }, None)
      }

      "all infix" in {
        val insert = List(19L, 14L, 13L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack }.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).linsertAfter(timeline, List(20L).pack, List(19L).pack)
          one(jredis).linsertAfter(timeline, List(16L).pack, List(14L).pack)
          one(jredis).linsertAfter(timeline, List(14L).pack, List(13L).pack)
        }

        redisShard.merge(timeline, insert.map { List(_).pack }, None)
      }

      "dupes" in {
        val insert = List(16L, 15L, 15L, 12L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(jredis).lrange(timeline, 0, -1) willReturn future
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn existing.map { List(_).pack }.toJavaList
          one(jredis).expire(timeline, 1)
          one(jredis).linsertAfter(timeline, List(16L).pack, List(15L).pack)
        }

        redisShard.merge(timeline, insert.map { List(_).pack }, None)
      }
    }

    "store" in {
      val entry1 = List(23L).pack
      val entry2 = List(20L).pack
      val entry3 = List(19L).pack

      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).rpush("generated-name", entry1)
        one(jredis).expire("generated-name", 15)
        one(jredis).rpush("generated-name", entry2)
        one(jredis).rpush("generated-name", entry3)
        one(jredis).rename("generated-name", timeline)
        one(jredis).expire(timeline, 1)
      }

      redisShard.store(timeline, List(entry1, entry2, entry3))
    }

    "deleteTimeline" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).del(timeline) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 0
      }

      redisShard.deleteTimeline(timeline)
    }

    "exceptions are wrapped" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(jredis).lpushx(timeline, data) willThrow new IllegalStateException("aiee")
      }

      redisShard.append(data, timeline, None) must throwA[ShardException]
    }
  }
}
