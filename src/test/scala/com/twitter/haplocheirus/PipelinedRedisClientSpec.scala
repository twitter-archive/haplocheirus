package com.twitter.haplocheirus

import java.util.concurrent.{ExecutionException, Future, TimeUnit}
import java.util.{List => JList}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.xrayspecs.TimeConversions._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object PipelinedRedisClientSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "PipelinedRedisClient" should {
    val jredis = mock[JRedisPipeline]
    val queue = mock[ErrorHandlingJobQueue]
    val future = mock[JRedisFutureSupport.FutureStatus]
    val future2 = mock[Future[JList[Array[Byte]]]]
    val future3 = mock[Future[java.lang.Long]]
    var client: PipelinedRedisClient = null

    val timeline = "t1"
    val data = "rus".getBytes
    val data2 = "zim".getBytes
    val job = Jobs.Append(data, timeline)

    doBefore {
      client = new PipelinedRedisClient("localhost", 10, 1.second, 1.day) {
        override def makeRedisClient = jredis
        override protected def uniqueTimelineName(name: String) = name + "~1"
      }
    }

    "push" in {
      expect {
        one(jredis).lpushx(timeline, data) willReturn future
      }

      client.push(timeline, data, None)
      client.pipeline.toList mustEqual List(PipelinedRequest(future, None))
    }

    "pop" in {
      expect {
        one(jredis).lrem(timeline, data, 1) willReturn future3
        one(future3).get() willReturn 1L
      }

      client.pop(timeline, data, None)
      client.pipeline.toList.map { _.future.get() } mustEqual List(ResponseStatus.STATUS_OK)
    }

    "pushAfter" in {
      expect {
        one(jredis).lpushxafter(timeline, data, data2) willReturn future
      }

      client.pushAfter(timeline, data, data2, None)
      client.pipeline.toList mustEqual List(PipelinedRequest(future, None))
    }

    "get" in {
      val result = List("a".getBytes, "z".getBytes)

      expect {
        one(jredis).lrange(timeline, 5, 14) willReturn future2
        one(future2).get(1000, TimeUnit.MILLISECONDS) willReturn result.toJavaList
        one(jredis).expire("t1", 86400)
      }

      client.get(timeline, 5, 10).toList mustEqual result
    }

    "set" in {
      val entry1 = List(23L).pack
      val entry2 = List(20L).pack
      val entry3 = List(19L).pack

      expect {
        one(jredis).rpush(timeline + "~1", entry1) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        one(jredis).expire(timeline + "~1", 15) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        one(jredis).rpush(timeline + "~1", entry2) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        one(jredis).rpush(timeline + "~1", entry3) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        one(jredis).rename(timeline + "~1", timeline) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        one(jredis).expire(timeline, 86400) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
      }

      client.set(timeline, List(entry1, entry2, entry3))
    }

    "delete" in {
      val futureLong = mock[Future[Long]]

      expect {
        one(jredis).del(timeline) willReturn futureLong
        one(futureLong).get(1000, TimeUnit.MILLISECONDS) willReturn 0L
      }

      client.delete(timeline)
    }

    "finishRequest" in {
      val request = PipelinedRequest(future, Some(e => queue.putError(job)))

      "success" in {
        expect {
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
        }

        client.finishRequest(request)
      }

      "failure" in {
        expect {
          one(future).get(1000, TimeUnit.MILLISECONDS) willReturn new ResponseStatus(ResponseStatus.Code.ERROR, "I burped.")
          one(queue).putError(job)
        }

        client.finishRequest(request)
      }

      "exception" in {
        expect {
          one(future).get(1000, TimeUnit.MILLISECONDS) willThrow new ExecutionException(new Exception("I died."))
          one(queue).putError(job)
        }

        client.finishRequest(request)
      }
    }
  }
}
