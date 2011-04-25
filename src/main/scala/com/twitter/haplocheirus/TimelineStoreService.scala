package com.twitter.haplocheirus

import com.twitter.gizzard.{Future, Hash}
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{CopyJobFactory, JobScheduler, JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardOfflineException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.ostrich.Stats
import net.lag.logging.Logger


class TimelineStoreService(val nameServer: NameServer[HaplocheirusShard],
                           val scheduler: PrioritizingJobScheduler[JsonJob],
                           val multiPushScheduler: JobScheduler[jobs.MultiPush],
                           val copyFactory: CopyJobFactory[HaplocheirusShard],
                           val readPool: RedisPool,
                           val writePool: RedisPool,
                           val slowPool: RedisPool)
      extends JobInjector {

  private val log = Logger(getClass.getName)

  val writeQueue = scheduler(Priority.Write.id).queue

  def injectJob(job: jobs.FallbackJob) {
    injectJob(writeQueue, job)
  }

  def shutdown() {
    scheduler.shutdown()
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
    tm match {
      case None    => Stats.incr("timeline-miss")
      case Some(_) => Stats.incr("timeline-hit")
    }
    tm
  }

  def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean) = {
    noShardOffline(shardFor(timeline).getRange(timeline, fromId, toId, dedupe))
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
