package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobQueue, JsonCodec, JsonJob, JsonJobParser}
import org.apache.commons.codec.binary.Base64

abstract class RedisJobParser(errorQueue: JobQueue[JsonJob]) extends JsonJobParser[JsonJob] {
  def apply(codec: JsonCodec[JsonJob], attributes: Map[String, Any]): JsonJob = {
    val job = parse(attributes)
    job.onError { e => errorQueue.put(job) }
    job
  }

  def parse(attributes: Map[String, Any]): RedisJob
}

class AppendParser(errorQueue: JobQueue[JsonJob], nameServer: NameServer[HaplocheirusShard]) extends RedisJobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new Append(Base64.decodeBase64(attributes("entry").asInstanceOf[String]),
               attributes("timeline").asInstanceOf[String],
               nameServer)
  }
}

class RemoveParser(errorQueue: JobQueue[JsonJob], nameServer: NameServer[HaplocheirusShard]) extends RedisJobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new Remove(attributes("timeline").asInstanceOf[String],
               attributes("entries").asInstanceOf[Seq[String]].map(Base64.decodeBase64(_)),
               nameServer)
  }
}

class MergeParser(errorQueue: JobQueue[JsonJob], nameServer: NameServer[HaplocheirusShard]) extends RedisJobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new Merge(attributes("timeline").asInstanceOf[String],
              attributes("entries").asInstanceOf[Seq[String]].map(Base64.decodeBase64(_)),
              nameServer)
  }
}

class DeleteTimelineParser(errorQueue: JobQueue[JsonJob], nameServer: NameServer[HaplocheirusShard]) extends RedisJobParser(errorQueue) {
  def parse(attributes: Map[String, Any]) = {
    new DeleteTimeline(attributes("timeline").asInstanceOf[String], nameServer)
  }
}
