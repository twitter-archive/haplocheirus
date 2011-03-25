package com.twitter.haplocheirus.jobs

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{Codec, JobScheduler, JsonCodec, JsonJob, JsonJobParser}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.json.JsonQuoted
import org.apache.commons.codec.binary.Base64
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TIOStreamTransport
import thrift.TimelineStore

class MultiPushCodec(nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob])
      extends Codec[MultiPush] {
  def flatten(job: MultiPush): Array[Byte] = {
    // java. :(
    val stream = new ByteArrayOutputStream()
    val args = new TimelineStore.append_args(job.entryByteBuffer, job.timelinePrefix, job.timelineIds.toJavaList)
    args.write(new TBinaryProtocol(new TIOStreamTransport(stream)))
    stream.toByteArray
  }

  def inflate(data: Array[Byte]): MultiPush = {
    // java. :(
    val args = new TimelineStore.append_args()
    args.read(new TBinaryProtocol(new TIOStreamTransport(new ByteArrayInputStream(data))))
    new MultiPush(args.getEntry(), args.timeline_prefix, args.timeline_ids.toSeq.toArray, nameServer, scheduler)
  }
}

// temp for backward compat
class MultiPushParser(nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob]) extends JsonJobParser {
  def apply(attributes: Map[String, Any]) = {
    new MultiPush(Base64.decodeBase64(attributes("entry").toString),
                  attributes("timeline_prefix").asInstanceOf[String],
                  ByteBuffer.wrap(Base64.decodeBase64(attributes("timeline_ids").toString)).toLongArray,
                  nameServer, scheduler)
  }
}


case class MultiPush(entry: Array[Byte], timelinePrefix: String, timelineIds: Array[long],
                     nameServer: NameServer[HaplocheirusShard], scheduler: JobScheduler[JsonJob])
     extends JsonJob with JobInjector {
  protected def encodeBase64(data: Array[Byte]) = {
    JsonQuoted("\"" + Base64.encodeBase64String(data).replaceAll("\r\n", "") + "\"")
  }

  def entryByteBuffer: ByteBuffer = ByteBuffer.wrap(entry)

  def toMap = {
    Map("entry" -> encodeBase64(entry), "timeline_prefix" -> timelinePrefix,
        "timeline_ids" -> encodeBase64(timelineIds.pack.array()))
  }

  def apply() {
    timelineIds.foreach { id =>
      val job = Append(entry, timelinePrefix + id.toString, nameServer)
      injectJob(scheduler.errorQueue, job)
    }
  }
}

