package com.twitter.haplocheirus

import java.io.IOException
import java.util.Random
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

class PipelineBatcher(pipeline: Pipeline) extends Runnable {
  def run {
    while (pipeline.running.getCount > 0) {
      pipeline.staging.drainTo(pipeline.batch)

      val head = pipeline.batch.peek
      val sleepTime = if (head ne null) {
        pipeline.batchTimeout.inMillis - ((System.nanoTime/1000) - (head.startNanoTime/1000))
      } else {
        1000L // need to check if the latch is decremented
      }
      if ((pipeline.batch.size >= pipeline.batchSize) || (sleepTime < 0)) {
        drainBatch
      }
      val batchElement = pipeline.staging.poll(sleepTime, TimeUnit.MILLISECONDS)
      if (batchElement ne null) {
        pipeline.batch.offer(batchElement)
      }
    }
    pipeline.staging.drainTo(pipeline.batch)
    drainBatch
    pipeline.flushed.countDown
  }

  protected def drainBatch() {
    while (pipeline.batch.size > 0) {
      batchElementToPipeline(pipeline.batch.poll)
    }
  }

  protected def batchElementToPipeline(batchElement: BatchElement) {
    Stats.addTiming("redis-pipeline-batch-usec", ((System.nanoTime/1000) - (batchElement.startNanoTime/1000)).toInt)
    val pipelineElement = new PipelineElement(
      batchElement.redisCall(),
      batchElement.callback,
      batchElement.onError,
      System.nanoTime)
    pipeline.pipeline.offer(pipelineElement)
  }
}

class PipelineReader(pipeline: Pipeline) extends Runnable {
  protected val exceptionLog = Logger.get("exception")

  def run {
    while (pipeline.flushed.getCount > 0) {
      val head = pipeline.pipeline.poll(1000L, TimeUnit.MILLISECONDS)
      if (head ne null) {
        val succeeded = wrap(head, () => head.future.get(pipeline.futureTimeout.inMillis, TimeUnit.MILLISECONDS))
        Stats.addTiming("redis-pipeline-usec", ((System.nanoTime/1000) - (head.startNanoTime/1000)).toInt)
        if (succeeded) {
          wrap(head, { () => head.callback(head.future) })
        }
        pipeline.operationCount += 1
      }
    }
    while (pipeline.pipeline.size > 0) {
      val head = pipeline.pipeline.poll
      wrap(head, { () => head.callback(head.future) })
    }
    pipeline.completed.countDown
    pipeline.client.redisClient.quit()
  }

  protected def wrap(request: PipelineElement, f: () => Unit): Boolean = {
    val e = try {
      f()
      None
    } catch {
      case e: ExecutionException =>
        exceptionLog.error(e, "Error in jredis request from %s: %s", pipeline.hostname, e.getCause())
        Some(e)
      case e: ClientRuntimeException =>
        exceptionLog.error(e, "Redis client error from %s: %s", pipeline.hostname, e.getCause())
        pipeline.client.shutdown
        Some(e)
      case e: TimeoutException =>
        Stats.incr("redis-timeout")
        exceptionLog.warning(e, "Timeout waiting for redis response from %s: %s", pipeline.hostname, e.getCause())
        Some(e)
      case e: Throwable =>
        exceptionLog.error(e, "Unknown jredis error from %s: %s", pipeline.hostname, e)
        Some(e)
    }
    e match {
      case None => true
      case Some(e) => {
        pipeline.countError(pipeline.client)
        request.onError.foreach(_(e))
        false
      }
    }
  }
}

class Pipeline(val client: PipelinedRedisClient, val hostname: String, maxSize: Int,
               val futureTimeout: Duration, val batchSize: Int, val batchTimeout: Duration,
               val countError: PipelinedRedisClient => Unit) {
  var operationCount = 0

  val staging = new LinkedBlockingQueue[BatchElement]
  val batch = new LinkedBlockingDeque[BatchElement]
  val pipeline = new LinkedBlockingQueue[PipelineElement]

  val running = new CountDownLatch(1)
  val flushed = new CountDownLatch(1)
  val completed = new CountDownLatch(1)

  val pipelineBatcherThread = new Thread(new PipelineBatcher(this)).start()
  val pipelineReaderThread = new Thread(new PipelineReader(this)).start()

  def shutdown() {
    running.countDown
  }

  def offer(redisCall: () => Future[java.lang.Long],
             callback: Future[java.lang.Long] => Unit,
             onError: Option[Throwable => Unit]) {
    if (running.getCount == 0) {
      throw new TimeoutException("client shutdown")
    }
    staging.offer(new BatchElement(redisCall, callback, onError, System.nanoTime))
  }

  def size(): Int = {
    staging.size + batch.size + pipeline.size
  }

  def isFull(onError: Option[Throwable => Unit]) {
    if (size > maxSize) {
      val e = new TimeoutException
      onError.foreach(_(e))
      throw e
    }
  }
}

class PipelinedRedisClient(hostname: String, pipelineMaxSize: Int, batchSize: Int, batchTimeout: Duration,
                           timeout: Duration, keysTimeout: Duration, expiration: Duration,
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

  val pipeline = new Pipeline(this, hostname, pipelineMaxSize, timeout, batchSize, batchTimeout, countError)

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
    pipeline.completed.await()
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
