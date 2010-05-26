package com.twitter.haplocheirus

import java.util.concurrent.Future
import java.util.{List => JList}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisFutureSupport, JRedisPipeline}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object PipelinedRedisClientSpec extends Specification with JMocker with ClassMocker {
  "PipelinedRedisClient" should {
    val jredis = mock[JRedisPipeline]
    val queue = mock[ErrorHandlingJobQueue]
    val future = mock[JRedisFutureSupport.FutureStatus]
    val future2 = mock[Future[JList[Array[Byte]]]]
    var client: PipelinedRedisClient = null

    val timeline = "t1"
    val data = "rus".getBytes
    val job = Jobs.Append(data, List(timeline))

    doBefore {
      client = new PipelinedRedisClient("localhost", 10, queue) {
        override def makeRedisClient = jredis
      }
    }

    "push" in {
      expect {
        one(jredis).lpushx(timeline, data) willReturn future
      }

      client.push(timeline, data, job)
      client.pipeline.toList mustEqual List(PipelinedRequest(future, job))
    }

    "pop" in {
      expect {
        one(jredis).ldelete(timeline, data) willReturn future
      }

      client.pop(timeline, data, job)
      client.pipeline.toList mustEqual List(PipelinedRequest(future, job))
    }

    "get" in {
      val result = List("a".getBytes, "z".getBytes)

      expect {
        one(jredis).lrange(timeline, 5, 14) willReturn future2
        one(future2).get() willReturn result.toJavaList
      }

      client.get(timeline, 5, 10).toList mustEqual result
    }

    "finishRequest" in {
      val request = PipelinedRequest(future, job)

      "success" in {
        expect {
          one(future).get() willReturn ResponseStatus.STATUS_OK
        }

        client.finishRequest(request)
      }

      "failure" in {
        expect {
          one(future).get() willReturn new ResponseStatus(ResponseStatus.Code.ERROR, "I burped.")
          one(queue).putError(job)
        }

        client.finishRequest(request)
      }
    }
  }
}
