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
    var client: PipelinedRedisClient = null

    val timeline = "t1"
    val data = "rus".getBytes
    val job = Jobs.Append(data, timeline)

    doBefore {
      client = new PipelinedRedisClient("localhost", 10, 1.second, 1.day) {
        override def makeRedisClient = jredis
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
        one(jredis).ldelete(timeline, data) willReturn future
      }

      client.pop(timeline, data, None)
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

    "delete" in {
      expect {
        one(jredis).del(timeline) willReturn future
        one(future).get(1000, TimeUnit.MILLISECONDS) willReturn ResponseStatus.STATUS_OK
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
