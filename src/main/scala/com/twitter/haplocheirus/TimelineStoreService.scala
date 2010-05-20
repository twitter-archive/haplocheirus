package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.thrift.conversions.Sequences._


class TimelineStoreService(nameServer: NameServer[HaplocheirusShard], future: Future) {
  def append(entry: Array[Byte], timeline_ids: Seq[String]) {
    timeline_ids.parallel(future).map { timeline =>
      val sourceId = 0  // FIXME FNV1
      nameServer.findCurrentForwarding(0, sourceId).append(entry, timeline)
    }
  }
}
