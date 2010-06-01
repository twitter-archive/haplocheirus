package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.jobs.{BoundJobParser, PolymorphicJobParser}
import com.twitter.gizzard.nameserver.{BasicShardRepository, NameServer}
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.querulous.StatsCollector
import net.lag.configgy.ConfigMap
import net.lag.logging.{Logger, ThrottledLogger}


object Priority extends Enumeration {
  val Migrate = Value(1)
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
      Map(Priority.Write.id -> "write", Priority.Migrate.id -> "migrate"))

    val log = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt,
                                          config("throttled_log.rate").toInt)
    val replicationFuture = new Future("ReplicationFuture", config.configMap("replication_pool"))
    val redisPool = new RedisPool(config.configMap("redis"), scheduler(Priority.Write.id).queue)
    val shardRepository = new BasicShardRepository[HaplocheirusShard](
      new HaplocheirusShardAdapter(_), log, replicationFuture)
    shardRepository += ("com.twitter.haplocheirus.RedisShard" -> new RedisShardFactory(redisPool))

    val nameServer = NameServer(config.configMap("nameservers"), Some(statsCollector),
                                shardRepository, log, replicationFuture)
    nameServer.reload()

    val writeJobParser = new BoundJobParser(nameServer)
    val copyJobParser = new BoundJobParser((nameServer, scheduler(Priority.Migrate.id)))
    jobParser += ("haplocheirus".r, writeJobParser)
    jobParser += ("(Copy|Migrate|MetadataCopy|MetadataMigrate)".r, copyJobParser)

    scheduler.start()

    val future = new Future("TimelineStoreService", config.configMap("service_pool"))
    new TimelineStoreService(nameServer, scheduler, Jobs.RedisCopyFactory, redisPool, future, replicationFuture)
  }
}
