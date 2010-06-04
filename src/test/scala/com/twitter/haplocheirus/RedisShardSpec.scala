package com.twitter.haplocheirus

import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.gizzard.shards.{ShardException, ShardInfo}
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.configgy.Configgy
import org.jredis.ClientRuntimeException
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RedisShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "RedisShard" should {
    val client = mock[PipelinedRedisClient]
    val shardInfo = mock[ShardInfo]
    var redisPool: RedisPool = null
    var redisShard: RedisShard = null

    val config = Configgy.config.configMap("redis")
    val data = "hello".getBytes
    val timeline = "t1"

    doBefore {
      redisPool = new RedisPool(config) {
        override def withClient[T](hostname: String)(f: PipelinedRedisClient => T): T = f(client)
      }
      redisShard = new RedisShard(shardInfo, 1, Nil, redisPool) {
        override val RANGE_QUERY_PAGE_SIZE = 3
      }
    }

    "append" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(client).push(timeline, data, None)
      }

      redisShard.append(data, timeline, None)
    }

    "remove" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(client).pop(timeline, data, None)
      }

      redisShard.remove(data, timeline, None)
    }

    "get" in {
      "unique entries" in {
        val entry1 = List(23L).pack
        val entry2 = List(20L).pack
        val entry3 = List(19L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, 10) willReturn List(entry1, entry2, entry3)
        }

        redisShard.get(timeline, 0, 10, false) mustEqual List(entry1, entry2, entry3)
      }

      "with duplicates in the sort key" in {
        val entry1 = List(23L, 1L).pack
        val entry2 = List(23L, 2L).pack
        val entry3 = List(19L, 3L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, 10) willReturn List(entry1, entry2, entry3)
        }

        redisShard.get(timeline, 0, 10, false) mustEqual List(entry2, entry3)
      }

      "with duplicates in the dedupe key" in {
        val entry1 = List(23L, 1L).pack
        val entry2 = List(21L, 2L).pack
        val entry3 = List(19L, 1L).pack

        expect {
          allowing(shardInfo).hostname willReturn "host1"
          allowing(client).get(timeline, 0, 10) willReturn List(entry1, entry2, entry3)
        }

        redisShard.get(timeline, 0, 10, false) mustEqual List(entry1, entry2, entry3)
        redisShard.get(timeline, 0, 10, true) mustEqual List(entry2, entry3)
      }

      "with missing dedupe key" in {
        val entry1 = List(23L, 1L).pack
        val entry2 = List(21L).pack
        val entry3 = List(19L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          allowing(client).get(timeline, 0, 10) willReturn List(entry1, entry2, entry3)
        }

        redisShard.get(timeline, 0, 10, true) mustEqual List(entry1, entry2, entry3)
      }

      "doesn't spaz when there aren't enough bytes to uniqify" in {
        val entry1 = "a".getBytes
        val entry2 = "lots-o-bytes".getBytes
        val entry3 = "almost!".getBytes

        expect {
          one(shardInfo).hostname willReturn "host1"
          allowing(client).get(timeline, 0, 10) willReturn List(entry1, entry2, entry3)
        }

        redisShard.get(timeline, 0, 10, true) mustEqual List(entry1, entry2, entry3)
      }
    }

    "getRange" in {
      "with missing fromId" in {
        val entry1 = List(23L).pack
        val entry2 = List(20L).pack
        val entry3 = List(19L).pack

        expect {
          one(shardInfo).hostname willReturn "host1"
          allowing(client).get(timeline, 0, 3) willReturn List(entry1, entry2, entry3)
          allowing(client).get(timeline, 3, 3) willReturn List[Array[Byte]]()
        }

        redisShard.getSince(timeline, 10L, false) mustEqual List(entry1, entry2, entry3)
      }

      "with fromId" in {
        "in the first page" in {
          val entry1 = List(23L).pack
          val entry2 = List(20L).pack
          val entry3 = List(19L).pack

          expect {
            one(shardInfo).hostname willReturn "host1"
            allowing(client).get(timeline, 0, 3) willReturn List(entry1, entry2, entry3)
          }

          redisShard.getSince(timeline, 19L, false) mustEqual List(entry1, entry2)
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
            allowing(client).get(timeline, 0, 3) willReturn List(entry1, entry2, entry3)
            allowing(client).get(timeline, 3, 3) willReturn List(entry3, entry4, entry5)
          }

          redisShard.getSince(timeline, 13L, false) mustEqual List(entry1, entry2, entry3, entry4)
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
          allowing(client).get(timeline, 0, 3) willReturn List(entry1, entry2, entry3)
          allowing(client).get(timeline, 3, 3) willReturn List(entry3, entry4, entry5)
        }

        redisShard.getSince(timeline, 13L, false) mustEqual List(entry1, entry3, entry4)
      }
    }

    "merge" in {
      val existing = List(20L, 18L, 16L, 12L, 7L)

      "no existing timeline" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, -1) willReturn List[Array[Byte]]()
        }

        redisShard.merge(timeline, List(List(21L).pack), None)
      }

      "nothing to merge" in {
        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, -1) willReturn existing.map { List(_).pack }
        }

        redisShard.merge(timeline, List[Array[Byte]](), None)
      }

      "all prefix" in {
        val insert = List(29L, 28L, 21L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, -1) willReturn existing.map { List(_).pack }
          one(client).push(timeline, List(29L).pack, None)
          one(client).push(timeline, List(28L).pack, None)
          one(client).push(timeline, List(21L).pack, None)
        }

        redisShard.merge(timeline, insert.map { List(_).pack }, None)
      }

      "all postfix" in {
        val insert = List(5L, 2L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, -1) willReturn existing.map { List(_).pack }
          one(client).pushAfter(timeline, List(7L).pack, List(5L).pack, None)
          one(client).pushAfter(timeline, List(5L).pack, List(2L).pack, None)
        }

        redisShard.merge(timeline, insert.map { List(_).pack }, None)
      }

      "all infix" in {
        val insert = List(19L, 14L, 13L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, -1) willReturn existing.map { List(_).pack }
          one(client).pushAfter(timeline, List(20L).pack, List(19L).pack, None)
          one(client).pushAfter(timeline, List(16L).pack, List(14L).pack, None)
          one(client).pushAfter(timeline, List(14L).pack, List(13L).pack, None)
        }

        redisShard.merge(timeline, insert.map { List(_).pack }, None)
      }

      "dupes" in {
        val insert = List(16L, 15L, 15L, 12L)

        expect {
          one(shardInfo).hostname willReturn "host1"
          one(client).get(timeline, 0, -1) willReturn existing.map { List(_).pack }
          one(client).pushAfter(timeline, List(16L).pack, List(15L).pack, None)
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
        one(client).set(timeline, List(entry1, entry2, entry3))
      }

      redisShard.store(timeline, List(entry1, entry2, entry3))
    }

    "deleteTimeline" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(client).delete(timeline)
      }

      redisShard.deleteTimeline(timeline)
    }

    "exceptions are wrapped" in {
      expect {
        one(shardInfo).hostname willReturn "host1"
        one(client).push(timeline, data, None) willThrow new IllegalStateException("aiee")
      }

      val shard = new RedisShardFactory(redisPool).instantiate(shardInfo, 1, Nil)
      shard.append(data, timeline, None) must throwA[ShardException]
    }
  }
}
