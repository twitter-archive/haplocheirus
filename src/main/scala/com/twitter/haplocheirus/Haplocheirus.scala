package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.jobs.{BoundJobParser, PolymorphicJobParser}
import com.twitter.gizzard.nameserver.{BasicShardRepository, NameServer}
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.querulous.StatsCollector
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


object Priority extends Enumeration {
  val Copy = Value(1)
  val Write = Value(2)
}

object Haplocheirus {
  val statsCollector = new StatsCollector {
    def incr(name: String, count: Int) = Stats.incr(name, count)
    def time[A](name: String)(f: => A): A = Stats.time(name)(f)
  }

  def apply(config: ConfigMap): TimelineStoreService = {
    val jobParser = new PolymorphicJobParser
    val scheduler = PrioritizingJobScheduler(config.configMap("queue"), jobParser,
      Map(Priority.Write.id -> "write", Priority.Copy.id -> "copy"))

    val readPool = new RedisPool("read", config.configMap("redis.read"))
    val writePool = new RedisPool("write", config.configMap("redis.write"))

    val shardRepository = new BasicShardRepository[HaplocheirusShard](
      new HaplocheirusShardAdapter(_), None, 6.seconds)
    val shardFactory = new RedisShardFactory(readPool, writePool,
                                             config("redis.range_query_page_size").toInt,
                                             config.configMap("timeline_trim"))
    shardRepository += ("com.twitter.haplocheirus.RedisShard" -> shardFactory)

    val nameServer = NameServer(config.configMap("nameservers"), Some(statsCollector),
                                shardRepository, None)
    nameServer.reload()

    val writeQueue = scheduler(Priority.Write.id).queue
    jobParser += (("Append".r, new BoundJobParser(new jobs.AppendParser(writeQueue), nameServer)))
    jobParser += (("Remove".r, new BoundJobParser(new jobs.RemoveParser(writeQueue), nameServer)))
    jobParser += (("Merge".r, new BoundJobParser(new jobs.MergeParser(writeQueue), nameServer)))
    jobParser += (("DeleteTimeline".r, new BoundJobParser(new jobs.DeleteTimelineParser(writeQueue), nameServer)))
    jobParser += (("Copy".r, new BoundJobParser(jobs.RedisCopyParser, (nameServer, scheduler(Priority.Copy.id)))))
    jobParser += (("MultiPush".r, new BoundJobParser(jobs.MultiPushParser, (nameServer, scheduler(Priority.Write.id)))))

    scheduler.start()

    new TimelineStoreService(nameServer, scheduler, jobs.RedisCopyFactory, readPool, writePool)
  }
}
