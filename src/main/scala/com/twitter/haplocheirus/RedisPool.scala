package com.twitter.haplocheirus

import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import com.twitter.ostrich.Stats
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, ShardBlackHoleException}
import net.lag.logging.Logger
import org.jredis.ClientRuntimeException

class RedisPoolHealthTracker(config: RedisPoolHealthTrackerConfig) {
  val log = Logger(getClass.getName)

  val concurrentErrorMap = new ConcurrentHashMap[String, AtomicInteger]
  val concurrentDisabledMap = new ConcurrentHashMap[String, Time]

  def countError(hostname:String) = {
    var count = concurrentErrorMap.get(hostname)
    if (count eq null) {
      val newCount = new AtomicInteger()
      count = concurrentErrorMap.putIfAbsent(hostname, newCount)
      if (count eq null) {
        count = newCount
      }
    }
    val c = count.incrementAndGet
    if (c > config.autoDisableErrorLimit && !concurrentDisabledMap.containsKey(hostname)) {
      log.error("Autodisabling %s", hostname)
      concurrentDisabledMap.put(hostname, config.autoDisableDuration.fromNow)
    }
  }

  def countNonError(hostname: String) = {
    if (concurrentErrorMap.containsKey(hostname)) {
      try {
        concurrentErrorMap.remove(hostname)
      } catch {
        case e: NullPointerException => {}
      }
    }
  }

  def checkErrorCount(shardInfo: ShardInfo) = {
    val timeout = concurrentDisabledMap.get(shardInfo.hostname)
    if (!(timeout eq null)) {
      if (Time.now < timeout) {
        throw new ShardBlackHoleException(shardInfo.id)
      } else {
        try {
          concurrentDisabledMap.remove(shardInfo.hostname)
          log.error("Reenabling %s", shardInfo.hostname)
          countNonError(shardInfo.hostname) // To remove from the error map
        } catch {
          case e: NullPointerException => {}
        }
      }
    }
  }
}

class RedisPool(name: String, healthTracker: RedisPoolHealthTracker, config: RedisPoolConfig) {
  val log = Logger(getClass.getName)
  val exceptionLog = Logger.get("exception")

  val concurrentServerMap = new ConcurrentHashMap[String, PipelinedRedisClient]
  val serverMap = scala.collection.jcl.Map(concurrentServerMap)

  def makeClient(hostname: String) = {
    val timeout = config.timeoutMsec.milliseconds
    val keysTimeout = config.keysTimeoutMsec.milliseconds
    val expiration = config.expirationHours.hours
    new PipelinedRedisClient(hostname, config.pipeline, timeout, keysTimeout, expiration)
  }

  def get(shardInfo: ShardInfo): PipelinedRedisClient = {
    healthTracker.checkErrorCount(shardInfo)

    val hostname = shardInfo.hostname
    var client = concurrentServerMap.get(hostname);
    if(client eq null) {
      val newClient = makeClient(hostname)
      client = concurrentServerMap.putIfAbsent(hostname, newClient);
      if(client eq null) {
        client = newClient
      }
    }
    client
  }

  def throwAway(hostname: String, client: PipelinedRedisClient) {
    try {
      client.shutdown()
    } catch {
      case e: Throwable =>
        exceptionLog.warning(e, "Error discarding dead redis client: %s", e)
    }
    concurrentServerMap.remove(hostname)
  }

  def withClient[T](shardInfo: ShardInfo)(f: PipelinedRedisClient => T): T = {
    var client: PipelinedRedisClient = null
    val hostname = shardInfo.hostname
    try {
      client = Stats.timeMicros("redis-acquire-usec") { get(shardInfo) }
    } catch {
      case e: ShardBlackHoleException =>
        throw e
      case e =>
        healthTracker.countError(hostname)
        throw e
    }
    val r = try {
      f(client)
    } catch {
      case e: ClientRuntimeException =>
        exceptionLog.error(e, "Redis client error: %s", e)
        healthTracker.countError(hostname)
        throwAway(hostname, client)
        throw e
      case e: TimeoutException =>
        Stats.incr("redis-timeout")
        exceptionLog.warning(e, "Redis request timeout: %s", e)
        healthTracker.countError(hostname)
        throw e
      case e: Throwable =>
        exceptionLog.error(e, "Non-redis error: %s", e)
        healthTracker.countError(hostname)
        throw e
    } finally {
      // FIXME: Is this even necessary?
      if (!client.alive) {
        log.error("client not alive for %s", hostname)
      }
    }
    healthTracker.countNonError(hostname)
    r
  }

  def shutdown() {
    serverMap.foreach { case (hostname, client) =>
      try {
        client.shutdown()
      } catch {
        case e: Throwable =>
          exceptionLog.error(e, "Failed to shutdown client: %s", e)
      }
    }
    serverMap.clear()
  }

  override def toString = {
    "<RedisPool: %s>".format(serverMap.map { case (hostname, client) =>
      "%s".format(hostname)
    }.mkString(", "))
  }
}
