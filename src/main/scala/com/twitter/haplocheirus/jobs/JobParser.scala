package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.jobs.UnboundJobParser
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.ErrorHandlingJobQueue
import org.apache.commons.codec.binary.Base64


abstract class JobParser(errorQueue: ErrorHandlingJobQueue) extends UnboundJobParser[NameServer[HaplocheirusShard]] {
  def apply(attributes: Map[String, Any]) = {
    val job = parse(attributes)
    job.onError { e => errorQueue.putError(job) }
    job
  }

  def parse(attributes: Map[String, Any]): RedisJob
}

class AppendParser(errorQueue: ErrorHandlingJobQueue) extends JobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new Append(Base64.decodeBase64(attributes("entry").asInstanceOf[String]),
               attributes("timeline").asInstanceOf[String])
  }
}

class RemoveParser(errorQueue: ErrorHandlingJobQueue) extends JobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new Remove(attributes("timeline").asInstanceOf[String],
               attributes("entries").asInstanceOf[Seq[String]].map(Base64.decodeBase64(_)))
  }
}

class MergeParser(errorQueue: ErrorHandlingJobQueue) extends JobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new Merge(attributes("timeline").asInstanceOf[String],
              attributes("entries").asInstanceOf[Seq[String]].map(Base64.decodeBase64(_)))
  }
}

class DeleteTimelineParser(errorQueue: ErrorHandlingJobQueue) extends JobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new DeleteTimeline(attributes("timeline").asInstanceOf[String])
  }
}
