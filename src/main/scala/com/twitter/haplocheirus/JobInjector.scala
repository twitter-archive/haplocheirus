package com.twitter.haplocheirus

import com.twitter.ostrich.Stats
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JsonJob}
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardRejectedOperationException}
import net.lag.logging.Logger

trait JobInjector {
  private val log = Logger(getClass.getName)

  // can be overridden for tests.
  var addOnError = true

  def injectJob(errorQueue: JobQueue[JsonJob], job: jobs.FallbackJob) {
    // Note that we don't really inject the job, but run it synchronously.
    if (addOnError) {
      job.onError { e => errorQueue.put(job) }
    }

    try {
      job()
      Stats.incr("job-success-count")
    } catch {
      case e: ShardBlackHoleException =>
        Stats.incr("job-blackholed-count")
      case e: ShardRejectedOperationException =>
        Stats.incr("job-darkmoded-count")
        errorQueue.put(job)
      case e: Throwable =>
        Stats.incr("job-error-count")
        log.error(e, "Exception starting job %s: %s", job, e)
        errorQueue.put(job)
    }
  }
}
