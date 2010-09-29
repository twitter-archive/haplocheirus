package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobScheduler, JsonCodec, JsonJob, JsonJobParser}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.json.JsonQuoted
import org.apache.commons.codec.binary.Base64

class MultiPushParser(nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob]) extends JsonJobParser[JsonJob] {
  def apply(codec: JsonCodec[JsonJob], attributes: Map[String, Any]) = {
    new MultiPush(Base64.decodeBase64(attributes("entry").toString),
                  attributes("timeline_prefix").asInstanceOf[String],
                  Base64.decodeBase64(attributes("timeline_ids").toString).toLongArray,
                  nameServer, scheduler)
  }
}

case class MultiPush(entry: Array[Byte], timelinePrefix: String, timelineIds: Seq[Long],
                     nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob])
     extends JsonJob with JobInjector {
  type Environment = (NameServer[HaplocheirusShard], JobScheduler[JsonJob])

  protected def encodeBase64(data: Array[Byte]) = {
    JsonQuoted("\"" + Base64.encodeBase64String(data).replaceAll("\r\n", "") + "\"")
  }

  def toMap = {
    Map("entry" -> encodeBase64(entry), "timeline_prefix" -> timelinePrefix,
        "timeline_ids" -> encodeBase64(timelineIds.pack))
  }

  def apply() {
    timelineIds.foreach { id =>
      val job = Append(entry, timelinePrefix + id.toString, nameServer)
      injectJob(scheduler.queue, job)
    }
  }
}

