package com.twitter.haplocheirus.jobs

import scala.collection.mutable
import com.twitter.gizzard.Hash
import com.twitter.gizzard.jobs.{BoundJobParser, UnboundJob}
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import org.apache.commons.codec.binary.Base64


abstract class RedisJob extends UnboundJob[NameServer[HaplocheirusShard]] {
  var onErrorCallback: Option[Throwable => Unit] = None

  def onError(f: Throwable => Unit) {
    onErrorCallback = Some(f)
  }

  protected def encodeBase64(data: Array[Byte]) = {
    Base64.encodeBase64String(data).replaceAll("\r\n", "")
  }

  override def toString = "<%s: %s>".format(getClass.getName, toMap)
}

case class Append(entry: Array[Byte], timeline: String) extends RedisJob {
  def toMap = {
    Map("entry" -> encodeBase64(entry), "timeline" -> timeline)
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).append(timeline, List(entry), onErrorCallback)
  }
}

case class Remove(entry: Array[Byte], timeline: String) extends RedisJob {
  def toMap = {
    Map("entry" -> encodeBase64(entry), "timeline" -> timeline)
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).remove(timeline, List(entry), onErrorCallback)
  }
}

case class Merge(timeline: String, entries: Seq[Array[Byte]]) extends RedisJob {
  def toMap = {
    Map("timeline" -> timeline, "entries" -> entries.map(encodeBase64(_)))
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).merge(timeline, entries, onErrorCallback)
  }
}

case class MergeIndirect(destTimeline: String, sourceTimeline: String) extends RedisJob {
  def toMap = {
    Map("dest_timeline" -> destTimeline, "source_timeline" -> sourceTimeline)
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    val destShard = nameServer.findCurrentForwarding(0, Hash.FNV1A_64(destTimeline))
    val sourceShard = nameServer.findCurrentForwarding(0, Hash.FNV1A_64(sourceTimeline))
    sourceShard.getRaw(sourceTimeline) foreach { entries =>
      destShard.merge(destTimeline, entries, onErrorCallback)
    }
  }
}

case class UnmergeIndirect(destTimeline: String, sourceTimeline: String) extends RedisJob {
  def toMap = {
    Map("dest_timeline" -> destTimeline, "source_timeline" -> sourceTimeline)
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    val destShard = nameServer.findCurrentForwarding(0, Hash.FNV1A_64(destTimeline))
    val sourceShard = nameServer.findCurrentForwarding(0, Hash.FNV1A_64(sourceTimeline))
    sourceShard.getRaw(sourceTimeline) foreach { entries =>
      destShard.remove(destTimeline, entries, onErrorCallback)
    }
  }
}

case class DeleteTimeline(timeline: String) extends RedisJob {
  def toMap = {
    Map("timeline" -> timeline)
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).deleteTimeline(timeline)
  }
}
