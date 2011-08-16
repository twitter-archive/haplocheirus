package com.twitter.haplocheirus

import scala.collection.Map
import net.lag.configgy.{Config => CConfig}
import com.twitter.gizzard.config.{Future, KestrelScheduler, MemoryScheduler, Scheduler, TServer}
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.haplocheirus.jobs.MultiPushCodec
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

trait HaplocheirusServer extends TServer {
  var name = "haplo"
  var port = 7666
}

trait AdminConfig {
  def httpPort: Int
  def textPort: Int
  def timeSeries: Boolean
}

trait RedisPoolHealthTrackerConfig {
  def clientErrorLimit: Int
  def autoDisableErrorLimit: Int
  def autoDisableDuration: Duration
}

trait RedisPoolConfig {
  def poolSize: Int  //  number of connections per redis
  def pipeline: Int  //  max outstanding redis operations

  // operation timeout
  def timeoutMsec: Int
  def keysTimeoutMsec: Int

  // expiration on timelines
  def expirationHours: Int
}

trait RedisConfig {
  def poolHealthTrackerConfig: RedisPoolHealthTrackerConfig
  def readPoolConfig: RedisPoolConfig
  def writePoolConfig: RedisPoolConfig
  def slowPoolConfig: RedisPoolConfig
  def rangeQueryPageSize: Int
}

trait MultiGetPoolConfig {
  def timeout: Duration
  def corePoolSize: Int
  def maxPoolSize: Int
  def keepAliveTime: Long
  def maxQueueSize: Int
}

trait TimelineTrimBounds {
  def lower: Int
  def upper: Int
}

trait TimelineTrimConfig {
  def bounds: Map[String, TimelineTrimBounds]
}

trait MultiPushScheduler extends Scheduler {
}

trait HaplocheirusConfig extends gizzard.config.GizzardServer {
  def server: HaplocheirusServer

  def adminConfig: AdminConfig

  def redisConfig: RedisConfig
  def multiGetPoolConfig: MultiGetPoolConfig
  def timelineTrimConfig: TimelineTrimConfig

  // Yuck, but JobScheduler.apply() requires an old-style configgy map.
  def convertSchedulerConfig(conf: Scheduler): CConfig = {
    val ret = new CConfig

    val cconfig = new CConfig
    cconfig.setString("name", conf.name)
    cconfig.setInt("threads", conf.threads)
    cconfig.setInt("error_limit", conf.errorLimit)
    cconfig.setInt("strobe_interval", conf.errorStrobeInterval.inSeconds)
    cconfig.setInt("error_delay", conf.errorRetryDelay.inSeconds)
    cconfig.setInt("flush_limit", conf.perFlushItemLimit)
    cconfig.setDouble("jitter_rate", conf.jitterRate)

    cconfig.setString("job_queue", conf.jobQueueName)
    cconfig.setString("error_queue", conf.errorQueueName)

    conf.schedulerType match {
      case k: KestrelScheduler => {
        cconfig.setString("type", "kestrel")
        // TODO(benjy): Possibly pass through other kestrel queue params, as needed.
        // But really it would be better for gizzard to support thrift-serialized jobs
        // and make this whole thing obsolete.
        ret.setString("path", k.path)
      }
      case m: MemoryScheduler => {
        cconfig.setString("type", "memory")
        cconfig.setInt("size_limit", m.sizeLimit)
      }
    }

    ret.setConfigMap(conf.name, cconfig)
    ret
  }
}
