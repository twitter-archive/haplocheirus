package com.twitter.haplocheirus

import com.twitter.ostrich.Stats
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JsonJob}
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardRejectedOperationException}
import net.lag.logging.Logger

trait JobInjector {
  private val exceptionLog = Logger.get("exception")

  // can be overridden for tests.
  var addOnError = true

  def injectJob(errorQueue: JobQueue[JsonJob], errorLimit: Int, job: jobs.FallbackJob) {
    // Note that we don't really inject the job, but run it synchronously.
    if (addOnError) {
      job.onError { e =>
        job.errorCount += 1
        if (job.errorCount <= errorLimit) {
          errorQueue.put(job)
        }
      }
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
        exceptionLog.error(e, "Exception starting job %s: %s", job, e)
        job.onErrorCallback.foreach(_(e))
    }
  }
}
