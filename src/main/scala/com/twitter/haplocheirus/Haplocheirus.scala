package com.twitter.haplocheirus

import com.twitter.gizzard.Future
import com.twitter.gizzard.nameserver.{BasicShardRepository, NameServer}
import com.twitter.gizzard.shards._
import com.twitter.ostrich.{W3CStats}
import com.twitter.querulous.StatsCollector
import net.lag.configgy.ConfigMap
import net.lag.logging.{Logger, ThrottledLogger}


object Haplocheirus {
  def statsCollector(w3c: W3CStats) = {
    new StatsCollector {
      def incr(name: String, count: Int) = w3c.incr(name, count)
      def time[A](name: String)(f: => A): A = w3c.time(name)(f)
    }
  }

  def apply(config: ConfigMap, w3c: W3CStats): Haplocheirus = {
    val stats = statsCollector(w3c)

    val log = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt,
                                          config("throttled_log.rate").toInt)
    val replicationFuture = new Future("ReplicationFuture", config.configMap("replication_pool"))
    val shardRepository = new BasicShardRepository[HaplocheirusShard](
      new HaplocheirusShardAdapter(_), log, replicationFuture)
    shardRepository += ("com.twitter.haplocheirus.RedisShard" -> new RedisShardFactory(config))

    val nameServer = NameServer(config.configMap("nameservers"), Some(stats),
                                shardRepository, log, replicationFuture)
    nameServer.reload()

    new Haplocheirus()
  }
}

class Haplocheirus {
  //
}

/*


  val polymorphicJobParser = new PolymorphicJobParser
  val jobParser = new LoggingJobParser(Stats, w3c, new JobWithTasksParser(polymorphicJobParser))
  val scheduler = PrioritizingJobScheduler(config.configMap("edges.queue"), jobParser,
    Map(Priority.High.id -> "primary", Priority.Medium.id -> "copy", Priority.Low.id -> "slow"))


  val singleJobParser = new jobs.single.JobParser(forwardingManager, OrderedUuidGenerator)
  val multiJobParser  = new jobs.multi.JobParser(forwardingManager, scheduler)
  val copyJobParser   = new BoundJobParser((nameServer, scheduler(Priority.Medium.id)))

  val future = new Future("EdgesFuture", config.configMap("edges.future"))

  polymorphicJobParser += ("\\.jobs\\.(Copy|Migrate|MetadataCopy|MetadataMigrate)".r, copyJobParser)
  polymorphicJobParser += ("flockdb\\.jobs\\.single".r, singleJobParser)
  polymorphicJobParser += ("flockdb\\.jobs\\.multi".r, multiJobParser)

  scheduler.start()

  new FlockDB(new EdgesService(nameServer, forwardingManager, jobs.CopyFactory, scheduler, future))
}
*/
