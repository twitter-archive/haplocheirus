package com.twitter.haplocheirus.jobs

import com.twitter.gizzard.jobs.UnboundJobParser
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.NameServer
import org.apache.commons.codec.binary.Base64


abstract class JobParser extends UnboundJobParser[NameServer[HaplocheirusShard]]

object AppendParser extends JobParser {
  def apply(attributes: Map[String, Any]) = {
    new Append(Base64.decodeBase64(attributes("entry").asInstanceOf[String]),
               attributes("timeline").asInstanceOf[String])
  }
}

object RemoveParser extends JobParser {
  def apply(attributes: Map[String, Any]) = {
    new Remove(attributes("timeline").asInstanceOf[String],
               attributes("entries").asInstanceOf[Seq[String]].map(Base64.decodeBase64(_)))
  }
}

object MergeParser extends JobParser {
  def apply(attributes: Map[String, Any]) = {
    new Merge(attributes("timeline").asInstanceOf[String],
              attributes("entries").asInstanceOf[Seq[String]].map(Base64.decodeBase64(_)))
  }
}

object DeleteTimelineParser extends JobParser {
  def apply(attributes: Map[String, Any]) = {
    new DeleteTimeline(attributes("timeline").asInstanceOf[String])
  }
}
