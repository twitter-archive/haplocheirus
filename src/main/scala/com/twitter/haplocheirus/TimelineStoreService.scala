package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.thrift.conversions.Sequences._


class TimelineStoreService(val nameServer: NameServer[HaplocheirusShard],
                           val scheduler: PrioritizingJobScheduler,
                           val future: Future,
                           val replicationFuture: Future) {
  def shutdown() {
    scheduler.shutdown()
    future.shutdown()
    replicationFuture.shutdown()
  }

  def append(entry: Array[Byte], timelines: Seq[String]) {
    scheduler(Priority.Write.id)(Jobs.Append(entry, timelines))
  }
}
