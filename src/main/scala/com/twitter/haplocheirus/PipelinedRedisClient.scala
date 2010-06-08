package com.twitter.haplocheirus

import java.io.IOException
import java.util.Random
import java.util.concurrent.{ExecutionException, Future, TimeoutException, TimeUnit}
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.Duration
import net.lag.logging.Logger
import org.jredis._
import org.jredis.protocol.ResponseStatus
import org.jredis.ri.alphazero.{JRedisClient, JRedisPipeline}
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec


case class PipelinedRequest(future: Future[ResponseStatus], onError: Option[Throwable => Unit])

object PipelinedRedisClient {
  var mockedOutJRedisClient: Option[JRedisPipeline] = None
}

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
  var alive = true

  // allow tests to override.
  def makeRedisClient = {
    PipelinedRedisClient.mockedOutJRedisClient.getOrElse(new JRedisPipeline(connectionSpec))
  }

  val pipeline = new mutable.ListBuffer[PipelinedRequest]

  protected def uniqueTimelineName(name: String): String = {
    val newName = name + "~" + System.currentTimeMillis + "~" + (new Random().nextInt & 0x7fffffff)
    if (redisClient.exists(newName).get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Boolean]) {
      uniqueTimelineName(name)
    } else {
      newName
    }
  }

  abstract class WrappedFuture[T](future: Future[T]) extends Future[ResponseStatus] {
    protected def convert(rv: T): ResponseStatus

    def get() = convert(future.get())
    def get(timeout: Long, units: TimeUnit) = convert(future.get(timeout, units))
    def isDone() = future.isDone()
    def isCancelled() = future.isCancelled()
    def cancel(x: Boolean) = future.cancel(x)
    override def equals(x: Any) = future.equals(x)
  }

  def convertBoolFuture(future: Future[java.lang.Boolean]) = new WrappedFuture[java.lang.Boolean](future) {
    protected def convert(rv: java.lang.Boolean) = {
      if (rv == true) ResponseStatus.STATUS_OK else new ResponseStatus(ResponseStatus.Code.ERROR, "failed")
    }
  }

  def convertLongFuture(future: Future[java.lang.Long]) = new WrappedFuture[java.lang.Long](future) {
    protected def convert(rv: java.lang.Long) = {
      if (rv == 1) ResponseStatus.STATUS_OK else new ResponseStatus(ResponseStatus.Code.ERROR, "zero")
    }
  }

  def shutdown() {
    alive = false
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
    Stats.timeMicros("redis-push-usec") {
      pipeline += PipelinedRequest(redisClient.lpushx(timeline, entry), onError)
      checkPipeline()
    }
  }

  def pop(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit]) {
    Stats.timeMicros("redis-pop-usec") {
      pipeline += PipelinedRequest(convertLongFuture(redisClient.lrem(timeline, entry, 1)), onError)
      checkPipeline()
    }
  }

  def pushAfter(timeline: String, oldEntry: Array[Byte], newEntry: Array[Byte],
                onError: Option[Throwable => Unit]) {
    Stats.timeMicros("redis-pushafter-usec") {
      pipeline += PipelinedRequest(redisClient.lpushxafter(timeline, oldEntry, newEntry), onError)
      checkPipeline()
    }
  }

  def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]] = {
    Stats.timeMicros("redis-get-usec") {
      val rv = redisClient.lrange(timeline, offset, offset + length - 1).get(timeout.inMillis, TimeUnit.MILLISECONDS).toSeq
      if (rv.size > 0) {
        pipeline += PipelinedRequest(convertBoolFuture(redisClient.expire(timeline, expiration.inSeconds)), None)
        checkPipeline()
      }
      rv
    }
  }

  def set(timeline: String, entries: Seq[Array[Byte]]) {
    Stats.timeMicros("redis-set-usec") {
      val tempName = uniqueTimelineName(timeline)
      var didExpire = false
      entries.foreach { entry =>
        redisClient.rpush(tempName, entry).get(timeout.inMillis, TimeUnit.MILLISECONDS)
        if (!didExpire) {
          redisClient.expire(tempName, 15).get(timeout.inMillis, TimeUnit.MILLISECONDS)
          didExpire = true
        }
      }
      if (entries.size > 0) {
        redisClient.rename(tempName, timeline).get(timeout.inMillis, TimeUnit.MILLISECONDS)
        redisClient.expire(timeline, expiration.inSeconds).get(timeout.inMillis, TimeUnit.MILLISECONDS)
      }
    }
  }

  def delete(timeline: String) {
    Stats.timeMicros("redis-delete-usec") {
      redisClient.del(timeline).get(timeout.inMillis, TimeUnit.MILLISECONDS)
    }
  }
}
