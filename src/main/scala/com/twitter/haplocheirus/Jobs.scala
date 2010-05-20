package com.twitter.haplocheirus

import com.twitter.gizzard.jobs.{BoundJobParser, UnboundJob}
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import org.apache.commons.codec.binary.Base64


class JobParser(nameServer: NameServer[HaplocheirusShard]) extends BoundJobParser(nameServer)

case class Append(entry: Array[Byte], timelines: Seq[String]) extends UnboundJob[NameServer[HaplocheirusShard]] {
  // FIXME: json may be a poor choice for byte arrays.
  def toMap = {
    Map("entry" -> Base64.encodeBase64String(entry), "timeline_ids" -> timelines)
  }

  def this(attributes: Map[String, Any]) = {
    this(Base64.decodeBase64(attributes("entry").asInstanceOf[String]),
         attributes("timelines").asInstanceOf[Seq[String]])
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    // FIXME: parallelize into bunches per shard.
    timelines.foreach { timeline =>
      val sourceId = FNV1A_64(timeline) & 0x0fffffffffffffffL
      nameServer.findCurrentForwarding(0, sourceId).append(entry, timeline)
    }
  }
}
