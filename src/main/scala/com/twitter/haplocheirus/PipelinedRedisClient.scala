package com.twitter.haplocheirus

import java.util.concurrent.Future
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import net.lag.logging.Logger
import org.jredis._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisClient, JRedisPipeline}
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec


case class PipelinedRequest(future: Future[ResponseStatus], errorJob: Jobs.RedisJob)

/**
 * Thin wrapper around JRedisPipeline that will handle pipelining, and drop failed work into an
 * error queue.
 */
class PipelinedRedisClient(hostname: String, pipelineMaxSize: Int, queue: ErrorHandlingJobQueue) {
  val DEFAULT_PORT = 6379
  val log = Logger(getClass.getName)

  val segments = hostname.split(":", 2)
  val connectionSpec = if (segments.length == 2) {
    DefaultConnectionSpec.newSpec(segments(0), segments(1).toInt, 0, null)
  } else {
    DefaultConnectionSpec.newSpec(segments(0), DEFAULT_PORT, 0, null)
  }
  val redisClient = makeRedisClient

  // allow tests to override.
  def makeRedisClient = new JRedisPipeline(connectionSpec)

  val pipeline = new mutable.ListBuffer[PipelinedRequest]

  def finishRequest(request: PipelinedRequest) {
    val response = request.future.get()
    if (response.isError()) {
      log.error("Error response from %s: %s", hostname, response.message)
      queue.putError(request.errorJob)
    }
  }

  def checkPipeline() {
    while (pipeline.size > pipelineMaxSize) {
      finishRequest(pipeline.remove(0))
    }
  }

  def push(timeline: String, entry: Array[Byte], errorJob: Jobs.RedisJob) {
    pipeline += PipelinedRequest(redisClient.lpushx(timeline, entry), errorJob)
    checkPipeline()
  }

  def pop(timeline: String, entry: Array[Byte], errorJob: Jobs.RedisJob) {
    pipeline += PipelinedRequest(redisClient.ldelete(timeline, entry), errorJob)
    checkPipeline()
  }

  def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]] = {
    redisClient.lrange(timeline, offset, offset + length - 1).get().toSeq
  }
}
