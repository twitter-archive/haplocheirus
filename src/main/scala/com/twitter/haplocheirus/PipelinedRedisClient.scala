package com.twitter.haplocheirus

import java.io.IOException
import java.util.concurrent.{ExecutionException, Future, TimeoutException, TimeUnit}
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.xrayspecs.Duration
import net.lag.logging.Logger
import org.jredis._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisClient, JRedisPipeline}
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec


case class PipelinedRequest(future: Future[ResponseStatus], onError: Option[Throwable => Unit])

/**
 * Thin wrapper around JRedisPipeline that will handle pipelining, and call an error handler on
 * failure.
 */
class PipelinedRedisClient(hostname: String, pipelineMaxSize: Int, timeout: Duration,
                           expiration: Duration) {
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

  implicit def convertFuture(future: Future[java.lang.Boolean]) = new Future[ResponseStatus] {
    private def convert(rv: java.lang.Boolean) = ResponseStatus.STATUS_OK
    def get() = convert(future.get())
    def get(timeout: Long, units: TimeUnit) = convert(future.get(timeout, units))
    def isDone() = future.isDone()
    def isCancelled() = future.isCancelled()
    def cancel(x: Boolean) = future.cancel(x)
  }

  def shutdown() {
    while (pipeline.size > 0) {
      finishRequest(pipeline.remove(0))
    }
    redisClient.quit()
  }

  def finishRequest(request: PipelinedRequest) {
    try {
      val response = request.future.get(timeout.inMillis, TimeUnit.MILLISECONDS)
      if (response.isError()) {
        log.error("Error response from %s: %s", hostname, response.message)
        request.onError.foreach(_(new IOException("Redis error: " + response.message)))
      }
    } catch {
      case e: ExecutionException =>
        log.error(e.getCause(), "Error in jredis request from %s: %s", hostname, e.getCause())
        request.onError.foreach(_(e))
      case e: TimeoutException =>
        log.error(e, "Timeout waiting for redis response from %s: %s", hostname, e.getCause())
        request.onError.foreach(_(e))
      case e: Throwable =>
        log.error(e, "Unknown jredis error from %s: %s", hostname, e.getCause())
        request.onError.foreach(_(e))
    }
  }

  def checkPipeline() {
    while (pipeline.size > pipelineMaxSize) {
      finishRequest(pipeline.remove(0))
    }
  }

  def push(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit]) {
    pipeline += PipelinedRequest(redisClient.lpushx(timeline, entry), onError)
    checkPipeline()
  }

  def pop(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit]) {
    pipeline += PipelinedRequest(redisClient.ldelete(timeline, entry), onError)
    checkPipeline()
  }

  def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]] = {
    val rv = redisClient.lrange(timeline, offset, offset + length - 1).get(timeout.inMillis, TimeUnit.MILLISECONDS).toSeq
    if (rv.size > 0) {
      pipeline += PipelinedRequest(convertFuture(redisClient.expire(timeline, expiration.inSeconds)), None)
      checkPipeline()
    }
    rv
  }

  def delete(timeline: String) {
    redisClient.del(timeline).get(timeout.inMillis, TimeUnit.MILLISECONDS)
  }
}
