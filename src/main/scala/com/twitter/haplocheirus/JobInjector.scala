package com.twitter.haplocheirus

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JsonJob}
import net.lag.logging.Logger

trait JobInjector {
  private val log = Logger(getClass.getName)

  // can be overridden for tests.
  var addOnError = true

  def injectJob(errorQueue: JobQueue[JsonJob], job: jobs.RedisJob) {
    if (addOnError) {
      job.onError { e => errorQueue.put(job) }
    }

    try {
      job()
    } catch {
      case e: Throwable =>
        log.error(e, "Exception starting job %s: %s", job, e)
        errorQueue.put(job)
    }
  }
}
