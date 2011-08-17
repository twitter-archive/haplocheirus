package com.twitter.haplocheirus

import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, ExecutorService, ThreadPoolExecutor, TimeUnit, RejectedExecutionException, TimeoutException}
import com.twitter.gizzard.Hash
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{CopyJobFactory, JobScheduler, JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardOfflineException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.ostrich.Stats
import com.twitter.util.{Future, Promise, Try, Return, Throw}
import com.twitter.util.TimeConversions._

/* cut and pasted from newer util */
class ExecutorServiceFuturePool(val executor: ExecutorService) {
  def apply[T](f: => T): Future[T] = {
    val out = new Promise[T]
    executor.submit(new Runnable {
      def run = out.update(Try(f))
    })
    out
  }
}

class MultiGetPool(config: MultiGetPoolConfig) {
  var timeout = config.timeout
  val queue = new ArrayBlockingQueue[Runnable](config.maxQueueSize)
  val threadPool = new ThreadPoolExecutor(config.corePoolSize,
                                          config.maxPoolSize,
                                          config.keepAliveTime,
                                          TimeUnit.MILLISECONDS,
                                          queue)
  val futurePool = new ExecutorServiceFuturePool(threadPool)

  Stats.makeGauge("get-multi-pool-size") { threadPool.getPoolSize().toDouble }
  Stats.makeGauge("get-multi-queue-depth") { queue.size.toDouble }

  def submit[T](f: => T): Future[T] = futurePool(f)
  def shutdown() = threadPool.shutdown()
}

class TimelineStoreService(val nameServer: NameServer[HaplocheirusShard],
                           val scheduler: PrioritizingJobScheduler[JsonJob],
                           val multiPushScheduler: JobScheduler[jobs.MultiPush],
                           val copyFactory: CopyJobFactory[HaplocheirusShard],
                           val readPool: RedisPool,
                           val writePool: RedisPool,
                           val slowPool: RedisPool,
                           val multiGetPool: MultiGetPool)
      extends JobInjector {

  val writeQueue = scheduler(Priority.Write.id).queue
  val errorLimit = scheduler(Priority.Write.id).errorLimit

  def injectJob(job: jobs.FallbackJob) {
    injectJob(writeQueue, errorLimit, job)
  }

  def shutdown() {
    scheduler.shutdown()
    multiGetPool.shutdown()
    multiPushScheduler.shutdown()
    readPool.shutdown()
    writePool.shutdown()
    slowPool.shutdown()
  }

  private def shardFor(timeline: String) = {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline))
  }

  def noBlackHole[A](method: => A) = {
    try {
      method
    } catch {
      case e: ShardBlackHoleException => {}
    }
  }

  def noShardOffline[A](method: => Option[A]): Option[A] = {
    try {
      method
    } catch {
      case e: ShardOfflineException =>
        None
    }
  }

  def append(entry: Array[Byte], prefix: String, timelines: Seq[Long]) {
    Stats.addTiming("x-timelines-per-append", timelines.size)
    val job = Stats.timeMicros("x-append-job") {
      jobs.MultiPush(entry, prefix, timelines.toArray, nameServer, scheduler(Priority.Write.id))
    }
    Stats.timeMicros("x-append-put") {
      multiPushScheduler.queue.put(job)
    }
  }

  def remove(entry: Array[Byte], prefix: String, timelines: Seq[Long]) {
    Stats.addTiming("x-timelines-per-remove", timelines.size)
    timelines.foreach { timeline =>
      injectJob(jobs.Remove(prefix + timeline.toString, List(entry), nameServer))
    }
  }

  def filter(timeline: String, entries: Seq[Array[Byte]], maxSearch: Int) = {
    noShardOffline(shardFor(timeline).oldFilter(timeline, entries, maxSearch))
  }

  def filter2(timeline: String, entries: Seq[Long], maxSearch: Int) = {
    noShardOffline(shardFor(timeline).filter(timeline, entries, maxSearch))
  }

  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean) = {
    val tm = noShardOffline(shardFor(timeline).get(timeline, offset, length, dedupe))
    val timelineType = timeline.split(":")(0)
    tm match {
      case None    => {
        Stats.incr("timeline-miss")
        Stats.incr("timeline-" + timelineType + "-miss")
      }
      case Some(_) => {
        Stats.incr("timeline-hit")
        Stats.incr("timeline-" + timelineType + "-hit")
      }
    }
    tm
  }

  def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean) = {
    val tm = noShardOffline(shardFor(timeline).getRange(timeline, fromId, toId, dedupe))
    tm match {
      case None    => Stats.incr("timeline-miss")
      case Some(_) => Stats.incr("timeline-hit")
    }
    tm
  }

  def getMulti(gets: Seq[TimelineGet]) = {
    Stats.addTiming("get-multi-width", gets.size)
    val getter: TimelineGet => Option[TimelineSegment] = { command =>
      get(command.timeline_id, command.offset, command.length, command.dedupe)
    }
    getGenericMulti(gets, getter)
  }

  def getRangeMulti(gets: Seq[TimelineGetRange]) = {
    Stats.addTiming("get-range-multi-width", gets.size)
    val getter: TimelineGetRange => Option[TimelineSegment] = { command =>
      getRange(command.timeline_id, command.from_id, command.to_id, command.dedupe)
    }
    getGenericMulti(gets, getter)
  }

  def getGenericMulti[A](gets: Seq[A], getter: A => Option[TimelineSegment]) = {
    val timeoutLatch = new CountDownLatch(1)
    val futures = gets map { get =>
      try {
        multiGetPool.submit {
          if (timeoutLatch.getCount > 0) {
            getter(get)
          } else {
            Stats.incr("get-multi-expired-in-queue")
            throw new TimeoutException("Expired in queue")
          }
        }
      } catch {
        case e: RejectedExecutionException => {
          Stats.incr("get-multi-rejected")
          futureThrow(new TimeoutException("MultiGetPool rejected job"))
        }
      }
    }
    futureCollect(futures) within multiGetPool.timeout
    timeoutLatch.countDown
    futures map { _ within 0.seconds }
  }

  /* "collect" and "value" semi-cut-n-pasted from newer util */
  def futureCollect[A](fs: Seq[Future[A]]): Future[Seq[A]] = {
    val collected = fs.foldLeft(futureValue(Nil: List[A])) { case (a, e) =>
      a flatMap { aa => e map { _ :: aa } }
    } map { _.reverse }

    collected
  }

  def futureValue[A](a: A): Future[A] = {
    val value = new Promise[A]
    value.update(Return(a))
    value
  }

  def futureThrow(e: Throwable): Future[Nothing] = {
    new Promise { update(Throw(e)) }
  }

  def store(timeline: String, entries: Seq[Array[Byte]]) {
    noBlackHole(shardFor(timeline).store(timeline, entries))
  }

  def merge(timeline: String, entries: Seq[Array[Byte]]) {
    injectJob(jobs.Merge(timeline, entries, nameServer))
  }

  def unmerge(timeline: String, entries: Seq[Array[Byte]]) {
    injectJob(jobs.Remove(timeline, entries, nameServer))
  }

  def mergeIndirect(destTimeline: String, sourceTimeline: String): Boolean = {
    shardFor(sourceTimeline).getRaw(sourceTimeline) match {
      case None =>
        false
      case Some(entries) =>
        injectJob(jobs.Merge(destTimeline, entries, nameServer))
        true
    }
  }

  def unmergeIndirect(destTimeline: String, sourceTimeline: String): Boolean = {
    shardFor(sourceTimeline).getRaw(sourceTimeline) match {
      case None =>
        false
      case Some(entries) =>
        injectJob(jobs.Remove(destTimeline, entries, nameServer))
        true
    }
  }

  def deleteTimeline(timeline: String) {
    injectJob(jobs.DeleteTimeline(timeline, nameServer))
  }
}
