package com.twitter.haplocheirus

import com.twitter.gizzard.{Future, GizzardServer}
import com.twitter.gizzard.nameserver.{BasicShardRepository, NameServer}
import com.twitter.gizzard.proxy.ExceptionHandlingProxy
import com.twitter.gizzard.scheduler.{JobScheduler, JsonCodec, JsonJob, JsonJobLogger, PrioritizingJobScheduler}
import com.twitter.gizzard.shards._
import com.twitter.ostrich.Stats
import com.twitter.querulous.StatsCollector
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger


object Priority extends Enumeration {
  val Copy = Value(1)
  val Write = Value(2)
  val MultiPush = Value(3)
}

class Haplocheirus(config: HaplocheirusConfig) extends GizzardServer[HaplocheirusShard, JsonJob](config) {
  private val queryStats = new StatsCollector {
    def incr(k: String, c: Int) = Stats.incr(k, c)
    def time[T](k: String)(f: => T): T = Stats.time(k)(f)
  }

  val readWriteShardAdapter = new HaplocheirusShardAdapter(_)
  val jobPriorities         = List(Priority.Copy, Priority.Write, Priority.MultiPush).map(_.id)
  val copyPriority          = Priority.Copy.id
  val copyFactory           = new jobs.RedisCopyFactory(nameServer, jobScheduler(Priority.Copy.id))
  val poolHealthTracker = new RedisPoolHealthTracker(config.redisConfig.poolHealthTrackerConfig)
  val readPool = new RedisPool("read", poolHealthTracker, config.redisConfig.readPoolConfig)
  val writePool = new RedisPool("write", poolHealthTracker, config.redisConfig.writePoolConfig)
  //val slowPool = new RedisPool("slow", poolHealthTracker, config.redisConfig.slowPoolConfig)
  val slowPool = writePool
  val multiGetPool = new MultiGetPool(config.multiGetPoolConfig)

  val shardFactory = new RedisShardFactory(readPool, writePool, slowPool,
                                           config.redisConfig.rangeQueryPageSize,
                                           config.timelineTrimConfig)

  shardRepo += ("com.twitter.haplocheirus.RedisShard" -> shardFactory)

  val errorQueue = jobScheduler(Priority.Write.id).errorQueue
  jobCodec += ("Append".r, new jobs.AppendParser(errorQueue, nameServer))
  jobCodec += ("Remove".r, new jobs.RemoveParser(errorQueue, nameServer))
  jobCodec += ("Merge".r, new jobs.MergeParser(errorQueue, nameServer))
  jobCodec += ("DeleteTimeline".r, new jobs.DeleteTimelineParser(errorQueue, nameServer))
  jobCodec += ("Copy".r, new jobs.RedisCopyParser(nameServer, jobScheduler(Priority.Copy.id)))
  jobCodec += ("MultiPush".r, new jobs.MultiPushParser(nameServer, jobScheduler(Priority.Write.id)))
  // multipush gets its own queue.
  val multiPushScheduler =
    JobScheduler("multipush",
                 config.convertSchedulerConfig(config.jobQueues(Priority.MultiPush.id)),
                 new jobs.MultiPushCodec(nameServer, jobScheduler(Priority.Write.id)), None)

  val haploService = {
    new TimelineStore(new TimelineStoreService(nameServer, jobScheduler, multiPushScheduler,
                                               copyFactory, readPool, writePool, slowPool,
                                               multiGetPool))
  }

  lazy val haploThriftServer = {
    val processor = new thrift.TimelineStore.Processor(
      NuLoggingProxy[thrift.TimelineStore.Iface](
        Stats, "timelines",
        haploService))

    config.server(processor)
  }


  def start() {
    startGizzard()
    multiPushScheduler.start()

    val runnable = new Runnable { def run() { haploThriftServer.serve() } }
    new Thread(runnable, "HaploServerThread").start()
  }

  def shutdown(quiesce: Boolean) {
    haploThriftServer.stop()
    shutdownGizzard(quiesce)
  }
}


