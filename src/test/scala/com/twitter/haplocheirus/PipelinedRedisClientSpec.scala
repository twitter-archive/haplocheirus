package com.twitter.haplocheirus

import java.util.concurrent.Future
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
    var client: PipelinedRedisClient = null

    val timeline = "t1"
    val data = "rus".getBytes
    val job = Jobs.Append(data, List(timeline))

    doBefore {
      client = new PipelinedRedisClient("localhost", 10, queue) {
        override def makeRedisClient = jredis
      }
    }

    "execute" in {
      expect {
        one(jredis).rpush(timeline, data) willReturn future
      }

      client.execute(job) { _.rpush(timeline, data) }
      client.pipeline.toList mustEqual List(PipelinedRequest(future, job))
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
