package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.{BasicShardRepository, NameServer}
import com.twitter.gizzard.scheduler.{JobScheduler, JsonCodec, JsonJob, JsonJobLogger, PrioritizingJobScheduler}
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.querulous.StatsCollector
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


object Priority extends Enumeration {
  val Copy = Value(1)
  val Write = Value(2)
  val MultiPush = Value(3)
}

object Haplocheirus {
  private val log = Logger.get(getClass.getName)

  val statsCollector = new StatsCollector {
    def incr(name: String, count: Int) = Stats.incr(name, count)
    def time[A](name: String)(f: => A): A = Stats.time(name)(f)
  }

  def apply(config: ConfigMap): TimelineStoreService = {
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

    val codec = new JsonCodec[JsonJob]({ unparsable: Array[Byte] =>
      log.error("Unparsable job: %s", unparsable.map { n => "%02x".format(n.toInt & 0xff) }.mkString(", "))
    })

    val badJobQueue = new JsonJobLogger[JsonJob](Logger.get("bad_jobs"))
    val scheduler = PrioritizingJobScheduler(config.configMap("queue"), codec,
      Map(Priority.Write.id -> "write", Priority.Copy.id -> "copy"),
      Some(badJobQueue))

    val errorQueue = scheduler(Priority.Write.id).errorQueue
    codec += ("Append".r, new jobs.AppendParser(errorQueue, nameServer))
    codec += ("Remove".r, new jobs.RemoveParser(errorQueue, nameServer))
    codec += ("Merge".r, new jobs.MergeParser(errorQueue, nameServer))
    codec += ("DeleteTimeline".r, new jobs.DeleteTimelineParser(errorQueue, nameServer))
    codec += ("Copy".r, new jobs.RedisCopyParser(nameServer, scheduler(Priority.Copy.id)))
    codec += ("MultiPush".r, new jobs.MultiPushParser(nameServer, scheduler(Priority.Write.id)))

    // multipush gets its own queue.
    val multiPushScheduler =
      JobScheduler("multipush", config.configMap("queue"),
                   new jobs.MultiPushCodec(nameServer, scheduler(Priority.Write.id)), None)

    scheduler.start()
    multiPushScheduler.start()

    val copyFactory = new jobs.RedisCopyFactory(nameServer, scheduler(Priority.Copy.id))
    new TimelineStoreService(nameServer, scheduler, multiPushScheduler, copyFactory, readPool, writePool)
  }
}
