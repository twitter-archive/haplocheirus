package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{JobScheduler, PrioritizingJobScheduler}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object TimelineStoreServiceSpec extends Specification with JMocker with ClassMocker {
  "TimelineStoreService" should {
    val nameServer = mock[NameServer[HaplocheirusShard]]
    val scheduler = mock[PrioritizingJobScheduler]
    val jobScheduler = mock[JobScheduler]
    val future = mock[Future]
    val replicationFuture = mock[Future]
    var service: TimelineStoreService = null

    doBefore {
      service = new TimelineStoreService(nameServer, scheduler, Jobs.RedisCopyFactory, future, replicationFuture)
    }

    "append" in {
      val data = "hello".getBytes
      val timelines = List("t1", "t2")

      expect {
        one(scheduler).apply(Priority.Write.id) willReturn jobScheduler
        one(jobScheduler).apply(Jobs.Append(data, timelines))
      }

      service.append(data, timelines)
    }
  }
}
