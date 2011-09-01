package com.twitter.haplocheirus

import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.Timer
import scala.collection.mutable
import scala.util.Random
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

  def countError(hostname: String, client: PipelinedRedisClient) = {
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

    if (client ne null) {
      client.errorCount.incrementAndGet
    }
  }

  def countNonError(hostname: String, client: PipelinedRedisClient) = {
    if (concurrentErrorMap.containsKey(hostname)) {
      try {
        concurrentErrorMap.remove(hostname)
      } catch {
        case e: NullPointerException => {}
      }
    }
    client.errorCount.set(0)
  }

  def isErrored(hostname: String): Boolean = {
    val timeout = concurrentDisabledMap.get(hostname)
    if (!(timeout eq null)) {
      if (Time.now < timeout) {
        true
      } else {
        try {
          log.error("Reenabling %s", hostname)
          concurrentErrorMap.remove(hostname)
          concurrentDisabledMap.remove(hostname)
        } catch {
          case e: NullPointerException => {}
        }
        false
      }
    } else {
      false
    }
  }
}

class RedisPool(name: String, healthTracker: RedisPoolHealthTracker, config: RedisPoolConfig) {
  val log = Logger(getClass.getName)
  val exceptionLog = Logger.get("exception")

  val poolIndexGenerator = new Random
  val serverPool = (0 to config.poolSize-1).map { i =>
    new ConcurrentHashMap[String, PipelinedRedisClient]
  }.toArray
  val serverLock = new ConcurrentHashMap[String, Int]

  val timer = new Timer

  def makeClient(hostname: String) = {
    val timeout = config.timeoutMsec.milliseconds
    val keysTimeout = config.keysTimeoutMsec.milliseconds
    val expiration = config.expirationHours.hours
    new PipelinedRedisClient(hostname, config.pipeline, config.batchSize, config.batchTimeout, timeout,
                             keysTimeout, expiration, timer,
                             (client: PipelinedRedisClient) => healthTracker.countError(hostname, client))
  }

  def get(shardInfo: ShardInfo): PipelinedRedisClient = {
    val hostname = shardInfo.hostname
    val server = poolIndexGenerator.nextInt(config.poolSize)
    var client = serverPool(server).get(hostname);

    if ((client ne null) && (client.errorCount.get > config.clientErrorLimit)) {
      if (client.alive) {
        throwAway(hostname, client)
      }
    }

    if (healthTracker.isErrored(shardInfo.hostname)) {
      throw new ShardBlackHoleException(shardInfo.id)
    }

    if ((client ne null) && !client.alive) {
      serverPool(server).remove(hostname, client)
      client = null
    }

    if (client eq null) {
      val lock = serverLock.put(hostname, 1)
      if (lock == 1) {
        throw new TimeoutException
      }
      try {
        client = makeClient(hostname)
        serverPool(server).put(hostname, client)
      } finally {
        serverLock.remove(hostname)
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
  }

  def giveBack(hostname: String, client: PipelinedRedisClient) {
    if (!client.alive) {
      log.error("giveBack failed %s", hostname)
    }
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
        healthTracker.countError(hostname, client)
        throw e
    }
    val r = try {
      f(client)
    } catch {
      case e: ClientRuntimeException =>
        exceptionLog.error(e, "Redis client error: %s", e)
        healthTracker.countError(hostname, client)
        throwAway(hostname, client)
        throw e
      case e: TimeoutException =>
        Stats.incr("redis-timeout")
        exceptionLog.warning(e, "Redis request timeout: %s", e)
        healthTracker.countError(hostname, client)
        throw e
      case e: Throwable =>
        exceptionLog.error(e, "Non-redis error: %s", e)
        healthTracker.countError(hostname, client)
        throw e
    } finally {
      Stats.timeMicros("redis-release-usec") { giveBack(hostname, client) }
    }

    healthTracker.countNonError(hostname, client)
    r
  }

  def shutdown() {
    serverPool.map { concurrentServerMap =>
      val serverMap = scala.collection.jcl.Map(concurrentServerMap)
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
  }

  override def toString = {
    "<RedisPool: %s>".format(serverPool.map { scala.collection.jcl.Map(_).filter { _._2.alive }.map { case (hostname, client) =>
      "%s".format(hostname)
    }.mkString(", ")}.mkString(", "))
  }
}
