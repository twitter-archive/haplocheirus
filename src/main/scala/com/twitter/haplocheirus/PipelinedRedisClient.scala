package com.twitter.haplocheirus

import java.io.IOException
import java.util.{Random, Timer, TimerTask}
import java.util.concurrent.{ExecutionException, Future, TimeoutException, TimeUnit, LinkedBlockingDeque, LinkedBlockingQueue, CountDownLatch}
import java.util.concurrent.atomic.AtomicInteger
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

case class BatchElement(redisCall: () => Future[java.lang.Long],
                        callback: Future[java.lang.Long] => Unit,
                        onError: Option[Throwable => Unit],
                        startNanoTime: Long)

case class PipelineElement(future: Future[java.lang.Long],
                           callback: Future[java.lang.Long] => Unit,
                           onError: Option[Throwable => Unit],
                           startNanoTime: Long)

class Pipeline(client: PipelinedRedisClient, hostname: String, maxSize: Int,
               futureTimeout: Duration, batchSize: Int, batchTimeout: Duration,
               timer: Timer, countError: PipelinedRedisClient => Unit) extends Runnable {
  var operationCount = 0

  protected val batch = new LinkedBlockingQueue[BatchElement]
  protected val pipeline = new LinkedBlockingDeque[PipelineElement]
  protected val exceptionLog = Logger.get("exception")

  protected var timerTask: Option[TimerTask] = None

  private val running = new CountDownLatch(1)
  val completed = new CountDownLatch(1)

  def run {
    while (running.getCount > 0) {
      val head = pipeline.poll(1000L, TimeUnit.MILLISECONDS)
      if (head ne null) {
        wrap(head, { () =>
          try {
            head.future.get(1000L, TimeUnit.MILLISECONDS)
            Stats.addTiming("redis-pipeline-usec", ((System.nanoTime/1000) - (head.startNanoTime/1000)).toInt)
            head.callback(head.future)
            operationCount += 1
          } catch {
            case e: TimeoutException => pipeline.offerFirst(head)
          }
        })
      }
    }
    drainBatch(false)
    while (pipeline.size > 0) {
      val head = pipeline.poll
      wrap(head, { () => head.callback(head.future) })
    }
    client.redisClient.quit()
    completed.countDown
  }

  protected def drainBatch(recordTime: Boolean) {
    while (batch.size > 0) {
      val batchElement = batch.poll
      if (batchElement ne null) {
        if (recordTime) {
          Stats.addTiming("redis-pipeline-batch-usec", ((System.nanoTime/1000) - (batchElement.startNanoTime/1000)).toInt)
        }
        batchElementToPipeline(batchElement)
      }
    }
  }

  protected def batchElementToPipeline(batchElement: BatchElement) {
    val pipelineElement = new PipelineElement(
      batchElement.redisCall(),
      batchElement.callback,
      batchElement.onError,
      System.nanoTime)
    pipeline.offer(pipelineElement)
  }

  def shutdown() {
    running.countDown
  }

  def offer(redisCall: () => Future[java.lang.Long],
             callback: Future[java.lang.Long] => Unit,
             onError: Option[Throwable => Unit]) {
    if (running.getCount == 0) {
      throw new TimeoutException("client shutdown")
    }
    batch.offer(new BatchElement(redisCall, callback, onError, System.nanoTime))
    if (batch.size >= batchSize) {
      val task = timerTask
      timerTask = None
      try {
        task foreach { _.cancel }
      } catch {
        case e: IllegalStateException =>
      }
      drainBatch(true)
      Stats.incr("redis-pipeline-batch-drain-full")
    } else if (timerTask == None) {
      val task = new TimerTask {
        def run = {
          drainBatch(false)
          Stats.incr("redis-pipeline-batch-drain-timeout")
        }
      }
      timer.schedule(task, batchTimeout.inMillis)
      timerTask = Some(task)
    }
  }

  def size(): Int = {
    batch.size + pipeline.size
  }

  def isFull(onError: Option[Throwable => Unit]) {
    if (size > maxSize) {
      Stats.incr("redis-pipeline-full")
      val e = new TimeoutException
      onError.foreach(_(e))
      throw e
    }
  }

  protected def wrap(request: PipelineElement, f: () => Unit): Boolean = {
    val e = try {
      f()
      None
    } catch {
      case e: ExecutionException =>
        exceptionLog.error(e, "Error in jredis request from %s: %s", hostname, e.getCause())
        Some(e)
      case e: ClientRuntimeException =>
        exceptionLog.error(e, "Redis client error from %s: %s", hostname, e.getCause())
        client.shutdown
        Some(e)
      case e: TimeoutException =>
        Stats.incr("redis-timeout")
        exceptionLog.warning(e, "Timeout waiting for redis response from %s: %s", hostname, e.getCause())
        Some(e)
      case e: Throwable =>
        exceptionLog.error(e, "Unknown jredis error from %s: %s", hostname, e)
        Some(e)
    }
    e match {
      case None => true
      case Some(e) => {
        countError(client)
        request.onError.foreach(_(e))
        false
      }
    }
  }
}

class PipelinedRedisClient(hostname: String, pipelineMaxSize: Int, batchSize: Int, batchTimeout: Duration,
                           timeout: Duration, keysTimeout: Duration, expiration: Duration, timer: Timer,
                           countError: PipelinedRedisClient => Unit) {
  val DEFAULT_PORT = 6379
  val KEYS_KEY = "%keys"

  val segments = hostname.split(":", 2)
  val connectionSpec = if (segments.length == 2) {
    DefaultConnectionSpec.newSpec(segments(0), segments(1).toInt, 0, null)
  } else {
    DefaultConnectionSpec.newSpec(segments(0), DEFAULT_PORT, 0, null)
  }
  connectionSpec.setHeartbeat(300)
  connectionSpec.setSocketProperty(connector.Connection.Socket.Property.SO_CONNECT_TIMEOUT, 50)
  connectionSpec.setSocketProperty(connector.Connection.Socket.Property.TCP_NODELAY, 1)
  val redisClient = makeRedisClient
  val errorCount = new AtomicInteger()
  var alive = true

  // allow tests to override.
  def makeRedisClient = {
    PipelinedRedisClient.mockedOutJRedisClient.getOrElse(new JRedisPipeline(connectionSpec))
  }

  val pipeline = new Pipeline(this, hostname, pipelineMaxSize, timeout, batchSize, batchTimeout, timer, countError)
  val pipelineThread = new Thread(pipeline).start()

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
    pipeline.shutdown()
  }

  def laterWithErrorHandling(redisCall: () => Future[java.lang.Long], onError: Option[Throwable => Unit])(f: Future[java.lang.Long] => Unit) {
    pipeline.offer(redisCall, f, onError)
  }

  // def isMember(timeline: String, entry: Array[Byte]) = {
  //   Stats.timeMicros("redis-lismember") {
  //     redisClient.lismember(timeline, entry).get(timeout.inMillis, TimeUnit.MILLISECONDS)
  //   }
  // }

  def push(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit])(f: Long => Unit) {
    pipeline.isFull(onError)
    Stats.timeMicros("redis-push-usec") {
      val redisCall = { () => redisClient.rpushx(timeline, Array(entry): _*) }
      laterWithErrorHandling(redisCall, onError) { future =>
        f(future.get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Long])
      }
    }
  }

  def pop(timeline: String, entry: Array[Byte], onError: Option[Throwable => Unit]) {
    pipeline.isFull(onError)
    Stats.timeMicros("redis-pop-usec") {
      val redisCall = { () => redisClient.lrem(timeline, entry, 0) }
      laterWithErrorHandling(redisCall, onError) { future =>
        future.get(timeout.inMillis, TimeUnit.MILLISECONDS)
      }
    }
  }

  def pushAfter(timeline: String, oldEntry: Array[Byte], newEntry: Array[Byte],
                onError: Option[Throwable => Unit])(f: Long => Unit) {
    pipeline.isFull(onError)
    Stats.timeMicros("redis-pushafter-usec") {
      val redisCall = { () => redisClient.linsertBefore(timeline, oldEntry, newEntry) }
      laterWithErrorHandling(redisCall, onError) { future =>
        f(future.get(timeout.inMillis, TimeUnit.MILLISECONDS).asInstanceOf[Long])
      }
    }
  }

  def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]] = {
    val start = -1 - offset
    val end = if (length > 0) (start - length + 1) else 0
    Stats.timeMicros("redis-get-usec") {
      redisClient.lrange(timeline, end, start).get(timeout.inMillis, TimeUnit.MILLISECONDS).toSeq.reverse
    }
  }

  /**
   * Suitable for live shards. Builds up data and then renames it across atomically.
   */
  def setAtomically(timeline: String, entries: Seq[Array[Byte]]) {
    Stats.timeMicros("redis-set-usec") {
      val tempName = uniqueTimelineName(timeline)
      if (entries.length > 0) {
        redisClient.rpush(tempName, entries.last)

        if (entries.length > 1) {
          val slice = new Array[Array[Byte]](entries.length - 1)
          var idx = slice.length

          for (entry <- entries) {
            idx = idx - 1 // from slice.length - 1 to 0
            if (idx >= 0) slice(idx) = entry
          }

          redisClient.rpushx(tempName, slice: _*)
        }

        redisClient.rename(tempName, timeline).get(timeout.inMillis, TimeUnit.MILLISECONDS)
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
      redisClient.rpush(timeline, TimelineEntry.EmptySentinel)
    }
  }

  def setLive(timeline: String, entries: Seq[Array[Byte]]) {
    Stats.timeMicros("redis-setlive-usec") {
      if (entries.length > 0) {
        redisClient.lpushx(timeline, entries.toArray: _*).get(timeout.inMillis, TimeUnit.MILLISECONDS)
      }
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
