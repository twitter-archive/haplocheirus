package com.twitter.haplocheirus

import java.util.concurrent.{ExecutionException, Future, TimeUnit}
import java.util.{List => JList}
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JsonJob}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.TimeConversions._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object PipelinedRedisClientSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "PipelinedRedisClient" should {
    val jredis = mock[JRedisPipeline]
    val queue = mock[JobQueue[JsonJob]]
    val future = mock[JRedisFutureSupport.FutureStatus]
    val future2 = mock[Future[JList[Array[Byte]]]]
    val longFuture = mock[JRedisFutureSupport.FutureLong]
    val booleanFuture = mock[JRedisFutureSupport.FutureBoolean]
    val keyListFuture = mock[Future[JList[String]]]
    val nameServer = mock[NameServer[HaplocheirusShard]]
    var client: PipelinedRedisClient = null

    val timeline = "t1"
    val data = "rus".getBytes
    val data2 = "zim".getBytes
    val job = jobs.Append(data, timeline, nameServer)

    doBefore {
      client = new PipelinedRedisClient("localhost", 10, 1.second, 1.second, 1.day) {
        override def makeRedisClient = jredis
        override protected def uniqueTimelineName(name: String) = name + "~1"
      }
    }

    "laterWithErrorHandling" in {
      val onError = Some({ e: Throwable => queue.put(job) })

      "success" in {
        client.laterWithErrorHandling(onError) { }
        client.flushPipeline()
        client.pipeline.size mustEqual 0
      }

      "exception" in {
        expect {
          one(queue).put(job)
        }

        client.laterWithErrorHandling(onError) { throw new ExecutionException(new Exception("I died.")) }
        client.flushPipeline()
      }
    }

    // "isMember" in {
    //   val entry1 = List(23L).pack
    //   val entry2 = List(20L).pack
    //
    //   expect {
    //     one(jredis).lismember(timeline, entry1) willReturn booleanFuture
    //     one(booleanFuture).get(1000, TimeUnit.MILLISECONDS) willReturn true
    //     one(jredis).lismember(timeline, entry2) willReturn booleanFuture
    //     one(booleanFuture).get(1000, TimeUnit.MILLISECONDS) willReturn false
    //   }
    //
    //   client.isMember(timeline, entry1) mustEqual true
    //   client.isMember(timeline, entry2) mustEqual false
    // }

    "push" in {
      expect {
        one(jredis).rpushx(timeline, data) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 23L
      }

      var count = 0L
      client.push(timeline, data, None) { n => count = n }
      client.flushPipeline()
      count mustEqual 23
    }

    "pop" in {
      expect {
        one(jredis).lrem(timeline, data, 0) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 1L
      }

      client.pop(timeline, data, None)
      client.flushPipeline()
    }

    "pushAfter" in {
      expect {
        one(jredis).linsertBefore(timeline, data, data2) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 23L
      }

      var count = 0L
      client.pushAfter(timeline, data, data2, None) { n => count = n }
      client.flushPipeline()
      count mustEqual 23
    }

    "get" in {
      val result = List("a".getBytes, "z".getBytes)

      expect {
        one(jredis).lrange(timeline, -15, -6) willReturn future2
        one(future2).get(1000, TimeUnit.MILLISECONDS) willReturn result.reverse.toJavaList
        one(jredis).expire("t1", 86400)
      }

      client.get(timeline, 5, 10).toList mustEqual result
    }

    "setAtomically" in {
      val entry1 = List(23L).pack.array
      val entry2 = List(20L).pack.array
      val entry3 = List(19L).pack.array

      expect {
        one(jredis).rpush(timeline + "~1", entry3) willReturn longFuture
        one(jredis).rpushx(timeline + "~1", entry2) willReturn longFuture
        one(jredis).rpushx(timeline + "~1", entry1) willReturn longFuture
        one(jredis).rename(timeline + "~1", timeline) willReturn future
        one(jredis).expire(timeline, 86400) willReturn future
        one(future).get(5000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
      }

      client.setAtomically(timeline, List(entry1, entry2, entry3))
    }

    "setLiveStart" in {
      expect {
        one(jredis).del(timeline)
        one(jredis).rpush(timeline, new Array[Byte](0))
      }

      client.setLiveStart(timeline)
    }

    "setLive" in {
      val entry1 = List(23L).pack.array
      val entry2 = List(20L).pack.array
      val entry3 = List(19L).pack.array

      expect {
        one(jredis).lpushx(timeline, entry1)
        one(jredis).lpushx(timeline, entry2)
        one(jredis).lpushx(timeline, entry3)

        one(jredis).lrem(timeline, new Array[Byte](0), 1) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 0L
        one(jredis).expire(timeline, 86400) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
      }

      client.setLive(timeline, List(entry1, entry2, entry3))
    }

    "delete" in {
      expect {
        one(jredis).del(timeline) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 0L
      }

      client.delete(timeline)
    }

    "size" in {
      expect {
        one(jredis).llen(timeline) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 23L
      }

      client.size(timeline) mustEqual 23
    }

    "trim" in {
      expect {
        one(jredis).ltrim(timeline, -200, -1)
      }

      client.trim(timeline, 200)
    }

    "makeKeyList" in {
      val keys = List("a", "b", "c", "d")

      expect {
        one(jredis).keys() willReturn keyListFuture
        one(keyListFuture).get(1000, TimeUnit.MILLISECONDS) willReturn keys.toJavaList
        one(jredis).ltrim(client.KEYS_KEY, 1, 0)
        keys.foreach { key => one(jredis).rpush(client.KEYS_KEY, key) }
        one(jredis).llen(client.KEYS_KEY) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 4L
      }

      client.makeKeyList() mustEqual 4
    }

    "getKeys" in {
      val keys = List("a", "b", "c", "d")

      expect {
        one(jredis).lrange(client.KEYS_KEY, 0, 1) willReturn future2
        one(future2).get(1000, TimeUnit.MILLISECONDS) willReturn keys.slice(0, 2).map { _.getBytes }.toJavaList
        one(jredis).lrange(client.KEYS_KEY, 2, 3) willReturn future2
        one(future2).get(1000, TimeUnit.MILLISECONDS) willReturn keys.slice(2, 4).map { _.getBytes }.toJavaList
      }

      client.getKeys(0, 2).toList mustEqual List("a", "b")
      client.getKeys(2, 2).toList mustEqual List("c", "d")
    }

    "deleteKeyList" in {
      expect {
        one(jredis).del(client.KEYS_KEY) willReturn longFuture
        one(longFuture).get(1000, TimeUnit.MILLISECONDS) willReturn 0L
      }

      client.deleteKeyList()
    }
  }
}
