package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.thrift.conversions.Sequences._


object FNV1A_64 {
  val PRIME = 1099511628211L

  def apply(data: Array[Byte]): Long = {
    var i = 0
    val len = data.length
    var rv = 0xcbf29ce484222325L
    while (i < len) {
      rv = (rv ^ (data(i) & 0xff)) * PRIME
      i += 1
    }
    rv
  }

  def apply(data: String): Long = apply(data.getBytes())
}

class TimelineStoreService(nameServer: NameServer[HaplocheirusShard], future: Future) {
  def append(entry: Array[Byte], timeline_ids: Seq[String]) {
    timeline_ids.parallel(future).map { timeline =>
      val sourceId = FNV1A_64(timeline) & 0x0fffffffffffffffL
      nameServer.findCurrentForwarding(0, sourceId).append(entry, timeline)
    }
  }
}
