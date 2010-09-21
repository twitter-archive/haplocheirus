package com.twitter.haplocheirus

import com.twitter.gizzard.{Future, Hash}
import com.twitter.gizzard.jobs.CopyFactory
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.ostrich.Stats
import net.lag.logging.Logger


class TimelineStoreService(val nameServer: NameServer[HaplocheirusShard],
                           val scheduler: PrioritizingJobScheduler,
                           val copyFactory: CopyFactory[HaplocheirusShard],
                           val readPool: RedisPool,
                           val writePool: RedisPool) {
  val log = Logger(getClass.getName)
  val writeQueue = scheduler(Priority.Write.id).queue

  def shutdown() {
    scheduler.shutdown()
    readPool.shutdown()
    writePool.shutdown()
  }

  private def shardFor(timeline: String) = {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline))
  }

  // can be overridden for tests.
  var addOnError = true

  private def injectJob(job: jobs.RedisJob) {
    if (addOnError) {
      job.onError { e => writeQueue.putError(job) }
    }
    try {
      job(nameServer)
    } catch {
      case e: Throwable =>
        log.error(e, "Exception starting job %s: %s", job, e)
        writeQueue.putError(job)
    }
  }

  def append(entry: Array[Byte], prefix: String, timelines: Seq[Long]) {
    Stats.addTiming("x-timelines-per-append", timelines.size)
    writeQueue.put(jobs.MultiPush(entry, prefix, timelines))
  }

  def remove(entry: Array[Byte], prefix: String, timelines: Seq[Long]) {
    Stats.addTiming("x-timelines-per-remove", timelines.size)
    timelines.foreach { timeline =>
      injectJob(jobs.Remove(prefix + timeline.toString, List(entry)))
    }
  }

  def filter(timeline: String, entries: Seq[Array[Byte]], maxSearch: Int) = {
    shardFor(timeline).filter(timeline, entries, maxSearch)
  }

  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean) = {
    shardFor(timeline).get(timeline, offset, length, dedupe)
  }

  def getRange(timeline: String, fromId: Long, toId: Long, dedupe: Boolean) = {
    shardFor(timeline).getRange(timeline, fromId, toId, dedupe)
  }

  def store(timeline: String, entries: Seq[Array[Byte]]) {
    shardFor(timeline).store(timeline, entries)
  }

  def merge(timeline: String, entries: Seq[Array[Byte]]) {
    injectJob(jobs.Merge(timeline, entries))
  }

  def unmerge(timeline: String, entries: Seq[Array[Byte]]) {
    injectJob(jobs.Remove(timeline, entries))
  }

  def mergeIndirect(destTimeline: String, sourceTimeline: String): Boolean = {
    shardFor(sourceTimeline).getRaw(sourceTimeline) match {
      case None =>
        false
      case Some(entries) =>
        injectJob(jobs.Merge(destTimeline, entries))
        true
    }
  }

  def unmergeIndirect(destTimeline: String, sourceTimeline: String): Boolean = {
    shardFor(sourceTimeline).getRaw(sourceTimeline) match {
      case None =>
        false
      case Some(entries) =>
        injectJob(jobs.Remove(destTimeline, entries))
        true
    }
  }

  def deleteTimeline(timeline: String) {
    injectJob(jobs.DeleteTimeline(timeline))
  }
}
