package com.twitter.haplocheirus.jobs

import scala.collection.mutable
import com.twitter.gizzard.Hash
import com.twitter.gizzard.jobs.{BoundJobParser, Copy, CopyFactory, CopyParser, UnboundJob}
import com.twitter.gizzard.shards.ShardId
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
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).append(entry, timeline, onErrorCallback)
  }
}

case class Remove(entry: Array[Byte], timeline: String) extends RedisJob {
  def toMap = {
    Map("entry" -> encodeBase64(entry), "timeline" -> timeline)
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).remove(entry, timeline, onErrorCallback)
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

case class DeleteTimeline(timeline: String) extends RedisJob {
  def toMap = {
    Map("timeline" -> timeline)
  }

  def apply(nameServer: NameServer[HaplocheirusShard]) {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).deleteTimeline(timeline)
  }
}



// FIXME



object RedisCopy {
  type Cursor = Int

  val START = 0
  val END = -1
  val COPY_COUNT = 10000
}

object RedisCopyFactory extends CopyFactory[HaplocheirusShard] {
  def apply(sourceShardId: ShardId, destinationShardId: ShardId) = null
  //new RedisCopy(sourceShardId, destinationShardId, RedisCopy.START)
}

object RedisCopyParser extends CopyParser[HaplocheirusShard] {
  def apply(attributes: Map[String, Any]) = {
    new RedisCopy(
      ShardId(attributes("source_shard_hostname").toString, attributes("source_shard_table_prefix").toString),
      ShardId(attributes("destination_shard_hostname").toString, attributes("destination_shard_table_prefix").toString),
      attributes("cursor").asInstanceOf[AnyVal].toInt,
      attributes("count").asInstanceOf[AnyVal].toInt)
  }
}

class RedisCopy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: RedisCopy.Cursor, count: Int)
      extends Copy[HaplocheirusShard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: ShardId, destinationShardId: ShardId, cursor: RedisCopy.Cursor) =
    this(sourceShardId, destinationShardId, cursor, RedisCopy.COPY_COUNT)

  def copyPage(sourceShard: HaplocheirusShard, destinationShard: HaplocheirusShard, count: Int) = {
/*
    val (items, newCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.writeCopies(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == Copy.END) {
      None
    } else {
      Some(new Copy(sourceShardId, destinationShardId, newCursor, count))
    }
*/
    Some(new RedisCopy(sourceShardId, destinationShardId, cursor, count))
  }

  def serialize = Map("cursor" -> cursor)
}
