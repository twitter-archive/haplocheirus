package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.jobs.CopyFactory
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.logging.Logger


class TimelineStoreService(val nameServer: NameServer[HaplocheirusShard],
                           val scheduler: PrioritizingJobScheduler,
                           val copyFactory: CopyFactory[HaplocheirusShard],
                           val redisPool: RedisPool,
                           val future: Future,
                           val replicationFuture: Future) {
  val log = Logger(getClass.getName)
  val writeQueue = scheduler(Priority.Write.id).queue

  def shutdown() {
    scheduler.shutdown()
    future.shutdown()
    replicationFuture.shutdown()
    redisPool.shutdown()
  }

  private def shardFor(timeline: String) = {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline))
  }

  // can be overridden for tests.
  var addOnError = true

  private def injectJob(job: Jobs.RedisJob) {
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

  def append(entry: Array[Byte], timelines: Seq[String]) {
    timelines.foreach { timeline =>
      injectJob(Jobs.Append(entry, timeline))
    }
  }

  def remove(entry: Array[Byte], timelines: Seq[String]) {
    timelines.foreach { timeline =>
      injectJob(Jobs.Remove(entry, timeline))
    }
  }

  def get(timeline: String, offset: Int, length: Int, dedupe: Boolean) = {
    shardFor(timeline).get(timeline, offset, length, dedupe)
  }

  def getSince(timeline: String, fromId: Long, dedupe: Boolean) = {
    shardFor(timeline).getSince(timeline, fromId, dedupe)
  }

  def deleteTimeline(timeline: String) {
    injectJob(Jobs.DeleteTimeline(timeline))
  }
}
