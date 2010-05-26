package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.jobs.CopyFactory
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.thrift.conversions.Sequences._


class TimelineStoreService(val nameServer: NameServer[HaplocheirusShard],
                           val scheduler: PrioritizingJobScheduler,
                           val copyFactory: CopyFactory[HaplocheirusShard],
                           val future: Future,
                           val replicationFuture: Future) {
  def shutdown() {
    scheduler.shutdown()
    future.shutdown()
    replicationFuture.shutdown()
  }

  private def shardFor(timeline: String) = {
    nameServer.findCurrentForwarding(0, Hash.FNV1A_64(timeline))
  }

  def append(entry: Array[Byte], timelines: Seq[String]) {
    Jobs.Append(entry, timelines)(nameServer)
  }

  def remove(entry: Array[Byte], timelines: Seq[String]) {
    Jobs.Remove(entry, timelines)(nameServer)
  }

  def get(timeline: String, offset: Int, length: Int) = {
    shardFor(timeline).get(timeline, offset, length)
  }
}
