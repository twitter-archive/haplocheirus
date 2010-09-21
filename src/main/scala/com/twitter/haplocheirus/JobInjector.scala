package com.twitter.haplocheirus

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import net.lag.logging.Logger

trait JobInjector {
  private val log = Logger(getClass.getName)

  // can be overridden for tests.
  var addOnError = true

  def injectJob(nameServer: NameServer[HaplocheirusShard], writeQueue: ErrorHandlingJobQueue, job: jobs.RedisJob) {
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
}
