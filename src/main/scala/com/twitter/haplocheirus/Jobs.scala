package com.twitter.haplocheirus.jobs

import scala.collection.mutable
import com.twitter.gizzard.Hash
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.JsonJob
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import org.apache.commons.codec.binary.Base64

abstract class FallbackJob extends JsonJob {
  var onErrorCallback: Option[Throwable => Unit] = None

  def onError(f: Throwable => Unit) {
    onErrorCallback = Some(f)
  }

  protected def encodeBase64(data: Array[Byte]) = {
    Base64.encodeBase64String(data).replaceAll("\r\n", "")
  }

  override def toString = "<%s: %s>".format(getClass.getName, toMap)
}

case class Append(entry: Array[Byte], timeline: String, nameServer: NameServer[HaplocheirusShard]) extends FallbackJob {
  def toMap = {
    Map("entry" -> encodeBase64(entry), "timeline" -> timeline)
  }

  def apply() {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).append(timeline, List(entry), onErrorCallback)
  }
}

case class Merge(timeline: String, entries: Seq[Array[Byte]], nameServer: NameServer[HaplocheirusShard]) extends FallbackJob {
  def toMap = {
    Map("timeline" -> timeline, "entries" -> entries.map(encodeBase64(_)))
  }

  def apply() {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).merge(timeline, entries, onErrorCallback)
  }
}

case class Remove(timeline: String, entries: Seq[Array[Byte]], nameServer: NameServer[HaplocheirusShard]) extends FallbackJob {
  def toMap = {
    Map("timeline" -> timeline, "entries" -> entries.map(encodeBase64(_)))
  }

  def apply() {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).remove(timeline, entries, onErrorCallback)
  }
}

case class DeleteTimeline(timeline: String, nameServer: NameServer[HaplocheirusShard]) extends FallbackJob {
  def toMap = {
    Map("timeline" -> timeline)
  }

  def apply() {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).deleteTimeline(timeline)
  }
}
