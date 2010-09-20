package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.jobs.{UnboundJob, UnboundJobParser}
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.gizzard.thrift.conversions.Sequences._
import org.apache.commons.codec.binary.Base64

object MultiPushParser extends UnboundJobParser[(NameServer[HaplocheirusShard], JobScheduler)] {
  def apply(attributes: Map[String, Any]) = {
    new MultiPush(Base64.decodeBase64(attributes("entry").asInstanceOf[String]),
                  attributes("timeline_prefix").asInstanceOf[String],
                  Base64.decodeBase64(attributes("timeline_ids").asInstanceOf[String]).toLongArray)
  }
}

case class MultiPush(entry: Array[Byte], timelinePrefix: String, timelineIds: Seq[Long])
     extends UnboundJob[(NameServer[HaplocheirusShard], JobScheduler)] {
  protected def encodeBase64(data: Array[Byte]) = {
    Base64.encodeBase64String(data).replaceAll("\r\n", "")
  }

  def toMap = {
    Map("entry" -> encodeBase64(entry), "timeline_prefix" -> timelinePrefix,
        "timeline_ids" -> encodeBase64(timelineIds.pack))
  }

  def apply(environment: (NameServer[HaplocheirusShard], JobScheduler)) {
    println("do eet")
    val (nameServer, scheduler) = environment
    timelineIds.foreach { id =>
      val job = Append(entry, timelinePrefix + id.toString)
      scheduler(job)
    }
  }
}

