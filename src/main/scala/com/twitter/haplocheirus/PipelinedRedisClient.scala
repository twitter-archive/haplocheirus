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

  val pipeline = new mutable.ListBuffer[() => Unit]

  protected def uniqueTimelineName(name: String): String = {
    val newName = name + "~" + System.currentTimeMillis + "~" + (new Random().nextInt & 0x7fffffff)
    if (redisClient.exists(newName).get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Boolean]) {
      uniqueTimelineName(name)
    } else {
      newName
    }
  }

  def shutdown() {
    alive = false
    flushPipeline()
    redisClient.quit()
  }

  def finishRequest(f: () => Unit) {
    try {
      f()
    } catch {
      case e: Throwable =>
        log.error(e, "Uncaught throwable in pipeline request: %s (eating it)", e)
    }
  }

  def checkPipeline() {
    while (pipeline.size > pipelineMaxSize) {
      finishRequest(pipeline.remove(0))
    }
  }

  def flushPipeline() {
    while (pipeline.size > 0) {
      finishRequest(pipeline.remove(0))
    }
  }

  def later(f: => Unit) {
    pipeline += { () => f }
    checkPipeline()
  }

  def laterWithErrorHandling(onError: Option[Throwable => Unit])(f: => Unit) {
    later {
      try {
        f
      } catch {
        case e: ExecutionException =>
          log.error(e.getCause(), "Error in jredis request from %s: %s", hostname, e.getCause())
          onError.foreach(_(e))
        case e: TimeoutException =>
          log.error(e, "Timeout waiting for redis response from %s: %s", hostname, e.getCause())
          onError.foreach(_(e))
        case e: Throwable =>
          log.error(e, "Unknown jredis error from %s: %s", hostname, e.getCause())
          onError.foreach(_(e))
      }
    }
  }

  def push(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit])(f: Long => Unit) {
    Stats.timeMicros("redis-push-usec") {
      val future = redisClient.lpushx(timeline, entry)
      laterWithErrorHandling(onError) {
        f(future.get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Long])
      }
    }
  }

  def pop(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit]) {
    Stats.timeMicros("redis-pop-usec") {
      val future = redisClient.lrem(timeline, entry, 0)
      laterWithErrorHandling(onError) {
        future.get(timeout.inMillis, TimeUnit.MILLISECONDS)
      }
    }
  }

  def pushAfter(timeline: String, oldEntry: Array[Byte], newEntry: Array[Byte],
                onError: Option[Throwable => Unit])(f: Long => Unit) {
    Stats.timeMicros("redis-pushafter-usec") {
      val future = redisClient.linsertAfter(timeline, oldEntry, newEntry)
      laterWithErrorHandling(onError) {
        f(future.get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Long])
      }
    }
  }

  def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]] = {
    Stats.timeMicros("redis-get-usec") {
      val rv = redisClient.lrange(timeline, offset, if (length > 0) (offset + length - 1) else -1).get(timeout.inMillis, TimeUnit.MILLISECONDS).toSeq
      if (rv.size > 0) {
        val future = redisClient.expire(timeline, expiration.inSeconds)
        later { future.get(timeout.inMillis, TimeUnit.MILLISECONDS) }
      }
      rv
    }
  }

  def set(timeline: String, entries: Seq[Array[Byte]]) {
    Stats.timeMicros("redis-set-usec") {
      val tempName = uniqueTimelineName(timeline)
      var didExpire = false
      entries.foreach { entry =>
        if (!didExpire) {
          redisClient.rpush(tempName, entry).get(timeout.inMillis, TimeUnit.MILLISECONDS)
          // bummer: we can't rename a key that has an expiration time, so these have to be permanent.
//          redisClient.expire(tempName, 15).get(timeout.inMillis, TimeUnit.MILLISECONDS)
          didExpire = true
        } else {
          redisClient.rpushx(tempName, entry).get(timeout.inMillis, TimeUnit.MILLISECONDS)
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

  def size(timeline: String) = {
    Stats.timeMicros("redis-llen-usec") {
      redisClient.llen(timeline).get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Long].toInt
    }
  }
}
