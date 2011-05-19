package com.twitter.haplocheirus

import java.io.IOException
import java.util.Random
import java.util.concurrent.{ExecutionException, Future, TimeoutException, TimeUnit, LinkedBlockingDeque}
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.ostrich.Stats
import com.twitter.util.Duration
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
                           keysTimeout: Duration, expiration: Duration) {
  val DEFAULT_PORT = 6379
  val KEYS_KEY = "%keys"
  val exceptionLog = Logger.get("exception")

  val segments = hostname.split(":", 2)
  val connectionSpec = if (segments.length == 2) {
    DefaultConnectionSpec.newSpec(segments(0), segments(1).toInt, 0, null)
  } else {
    DefaultConnectionSpec.newSpec(segments(0), DEFAULT_PORT, 0, null)
  }
  connectionSpec.setHeartbeat(300)
  val redisClient = makeRedisClient
  var alive = true

  // allow tests to override.
  def makeRedisClient = {
    PipelinedRedisClient.mockedOutJRedisClient.getOrElse(new JRedisPipeline(connectionSpec))
  }

  val pipeline = new LinkedBlockingDeque[(Future[java.lang.Long], Option[Throwable => Unit], () => Unit)]

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

  def finishRequest(onError: Option[Throwable => Unit], f: () => Unit) {
    try {
      f()
    } catch {
      case e: ExecutionException =>
        exceptionLog.error(e, "Error in jredis request from %s: %s", hostname, e.getCause())
        onError.foreach(_(e))
      case e: TimeoutException =>
        Stats.incr("redis-timeout")
        exceptionLog.warning(e, "Timeout waiting for redis response from %s: %s", hostname, e.getCause())
        onError.foreach(_(e))
      case e: Throwable =>
        exceptionLog.error(e, "Unknown jredis error from %s: %s", hostname, e)
        onError.foreach(_(e))
    }
  }

  def isPipelineFull(): Boolean = {
    var isFull = false
    var drained = false
    while (!isFull && !drained) {
      drained = true
      try {
        val head = pipeline.remove
        if (head._1.isDone) {
          finishRequest(head._2, head._3)
          drained = false
        } else {
          pipeline.putFirst(head)
          if (pipeline.size > pipelineMaxSize) {
            isFull = true
            Stats.incr("redis-pipeline-full")
          }
        }
      } catch {
        case e: NoSuchElementException => {}
      }
    }
    isFull
  }

  def flushPipeline() {
    while (pipeline.size > 0) {
      try {
        val (future, onError, next) = pipeline.poll
        finishRequest(onError, next)
      } catch {
        case e: NullPointerException => {}
      }
    }
  }

  def laterWithErrorHandling(future: Future[java.lang.Long], onError: Option[Throwable => Unit])(f: => Unit) {
    pipeline.offer((future, onError, () => f))
    isPipelineFull
  }

  // def isMember(timeline: String, entry: Array[Byte]) = {
  //   Stats.timeMicros("redis-lismember") {
  //     redisClient.lismember(timeline, entry).get(timeout.inMillis, TimeUnit.MILLISECONDS)
  //   }
  // }

  def push(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit])(f: Long => Unit) {
    if (isPipelineFull) {
      onError.foreach(_(new TimeoutException))
    } else {
      Stats.timeMicros("redis-push-usec") {
        val future = redisClient.rpushx(timeline, Array(entry): _*)
        laterWithErrorHandling(future, onError) {
          f(future.get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Long])
        }
      }
    }
  }

  def pop(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit]) {
    if (isPipelineFull) {
      onError.foreach(_(new TimeoutException))
    } else {
      Stats.timeMicros("redis-pop-usec") {
        val future = redisClient.lrem(timeline, entry, 0)
        laterWithErrorHandling(future, onError) {
          future.get(timeout.inMillis, TimeUnit.MILLISECONDS)
        }
      }
    }
  }

  def pushAfter(timeline: String, oldEntry: Array[Byte], newEntry: Array[Byte],
                onError: Option[Throwable => Unit])(f: Long => Unit) {
    if (isPipelineFull) {
      onError.foreach(_(new TimeoutException))
    } else {
      Stats.timeMicros("redis-pushafter-usec") {
        val future = redisClient.linsertBefore(timeline, oldEntry, newEntry)
        laterWithErrorHandling(future, onError) {
          f(future.get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Long])
        }
      }
    }
  }

  def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]] = {
    val start = -1 - offset
    val end = if (length > 0) (start - length + 1) else 0
    Stats.timeMicros("redis-get-usec") {
      val rv = redisClient.lrange(timeline, end, start).get(timeout.inMillis, TimeUnit.MILLISECONDS).toSeq.reverse
      if (rv.size > 0) {
        redisClient.expire(timeline, expiration.inSeconds)
      }
      rv
    }
  }

  /**
   * Suitable for live shards. Builds up data and then renames it across atomically.
   */
  def setAtomically(timeline: String, entries: Seq[Array[Byte]]) {
    Stats.timeMicros("redis-set-usec") {
      val tempName = uniqueTimelineName(timeline)
      if (entries.length > 0) {
        // FIXME: once RPUSH becomes variadic, do this in a single network call.
        redisClient.rpush(tempName, entries.last)
        // bummer: we can't rename a key that has an expiration time, so these have to be permanent.
        // FIXME: salvatore is gonna fix this in redis 2.2. bring this code back then.
        // redisClient.expire(tempName, 15).get(timeout.inMillis, TimeUnit.MILLISECONDS)
        if (entries.length > 1) {
          redisClient.rpushx(tempName, entries.slice(0, entries.length-1).reverse.toArray: _*)
        }
        redisClient.rename(tempName, timeline)
        redisClient.expire(timeline, expiration.inSeconds).get(timeout.inMillis, TimeUnit.MILLISECONDS)
      }
    }
  }

  /**
   * Suitable for copies and migrations of live data. Creates a stub that can get appends while
   * being filled in. Should be protected from reads until it's done.
   *
   * Call setLiveStart() to prepare an empty timeline for appends, then setLive to prepend the
   * existing data.
   */
  def setLiveStart(timeline: String) {
    Stats.timeMicros("redis-setlivestart-usec") {
      redisClient.del(timeline)
      redisClient.rpush(timeline, new Array[Byte](0))
    }
  }

  def setLive(timeline: String, entries: Seq[Array[Byte]]) {
    Stats.timeMicros("redis-setlive-usec") {
      redisClient.lpushx(timeline, entries.toArray: _*)
      redisClient.lrem(timeline, new Array[Byte](0), 1).get(timeout.inMillis, TimeUnit.MILLISECONDS)
      redisClient.expire(timeline, expiration.inSeconds).get(timeout.inMillis, TimeUnit.MILLISECONDS)
      0
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

  def trim(timeline: String, size: Int) {
    Stats.timeMicros("redis-ltrim-usec") {
      redisClient.ltrim(timeline, -size, -1)
    }
  }

  def makeKeyList() = {
    Stats.timeMicros("redis-keys") {
      val keyList = redisClient.keys().get(keysTimeout.inMillis, TimeUnit.MILLISECONDS).toSeq
      redisClient.ltrim(KEYS_KEY, 1, 0)
      keyList.foreach { key =>
    	redisClient.rpush(KEYS_KEY, key)
      }
      // force a pipeline flush too.
      size(KEYS_KEY)
    }
  }

  def getKeys(offset: Int, count: Int) = {
    Stats.timeMicros("redis-getkeys-usec") {
      redisClient.lrange(KEYS_KEY, offset, offset + count - 1).get(timeout.inMillis, TimeUnit.MILLISECONDS).toSeq.map { new String(_) }
    }
  }

  def deleteKeyList() {
    delete(KEYS_KEY)
  }
}
