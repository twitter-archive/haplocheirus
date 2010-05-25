package com.twitter.haplocheirus

import scala.collection.mutable
import com.twitter.gizzard.jobs.{BoundJobParser, Copy, CopyFactory, UnboundJob}
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import org.apache.commons.codec.binary.Base64


class JobParser(nameServer: NameServer[HaplocheirusShard]) extends BoundJobParser(nameServer)

object Jobs {
  abstract class RedisJob extends UnboundJob[NameServer[HaplocheirusShard]]

  case class Append(entry: Array[Byte], timelines: Seq[String]) extends RedisJob {
    def this(attributes: Map[String, Any]) = {
      this(Base64.decodeBase64(attributes("entry").asInstanceOf[String]),
           attributes("timelines").asInstanceOf[Seq[String]])
    }

    def toMap = {
      Map("entry" -> Base64.encodeBase64String(entry), "timelines" -> timelines)
    }

    def apply(nameServer: NameServer[HaplocheirusShard]) {
      timelines.foreach { timeline =>
        nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).append(entry, timeline)
      }
    }
  }

  case class Remove(entry: Array[Byte], timelines: Seq[String]) extends RedisJob {
    def this(attributes: Map[String, Any]) = {
      this(Base64.decodeBase64(attributes("entry").asInstanceOf[String]),
           attributes("timelines").asInstanceOf[Seq[String]])
    }

    def toMap = {
      Map("entry" -> Base64.encodeBase64String(entry), "timelines" -> timelines)
    }

    def apply(nameServer: NameServer[HaplocheirusShard]) {
      timelines.foreach { timeline =>
        nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline)).remove(entry, timeline)
      }
    }
  }

  object RedisCopyFactory extends CopyFactory[HaplocheirusShard] {
    def apply(sourceShardId: Int, destinationShardId: Int) = null
    //new RedisCopy(sourceShardId, destinationShardId, RedisCopy.START)
  }

  type Cursor = Int
  val COPY_COUNT = 1000

  class RedisCopy(sourceShardId: Int, destinationShardId: Int, cursor: Cursor, count: Int)
        extends Copy[HaplocheirusShard](sourceShardId, destinationShardId, count) {
    def this(sourceShardId: Int, destinationShardId: Int, cursor: Cursor) =
      this(sourceShardId, destinationShardId, cursor, Jobs.COPY_COUNT)

    def this(attributes: Map[String, AnyVal]) = {
      this(
        attributes("source_shard_id").toInt,
        attributes("destination_shard_id").toInt,
        attributes("cursor").toInt,
        attributes("count").toInt)
    }

    def copyPage(sourceShard: HaplocheirusShard, destinationShard: HaplocheirusShard, count: Int) = {
      Some(new RedisCopy(sourceShardId, destinationShardId, cursor, count))
    }

    def serialize = Map("cursor" -> cursor)
  }
}
