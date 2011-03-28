package com.twitter.haplocheirus

import java.util.concurrent.{LinkedBlockingQueue, TimeoutException, TimeUnit, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import com.twitter.ostrich.Stats
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, ShardBlackHoleException}
import net.lag.logging.Logger
import org.jredis.ClientRuntimeException


class RedisPool(name: String, config: RedisPoolConfig) {
  case class ClientPool(available: LinkedBlockingQueue[PipelinedRedisClient], val count: AtomicInteger)

  val log = Logger(getClass.getName)

  val poolTimeout = config.poolTimeoutMsec.toInt.milliseconds
  val concurrentServerMap = new ConcurrentHashMap[String, ClientPool]
  val concurrentErrorMap = new ConcurrentHashMap[String, AtomicInteger]
  val concurrentDisabledMap = new ConcurrentHashMap[String, Time]
  val serverMap = scala.collection.jcl.Map(concurrentServerMap)

  Stats.makeGauge("redis-pool-" + name) {
    serverMap.values.foldLeft(0) { _ + _.available.size }
  }

  def makeClient(hostname: String) = {
    val timeout = config.timeoutMsec.milliseconds
    val keysTimeout = config.keysTimeoutMsec.milliseconds
    val expiration = config.expirationHours.hours
    new PipelinedRedisClient(hostname, config.pipeline, timeout, keysTimeout, expiration)
  }

  def get(shardInfo: ShardInfo): PipelinedRedisClient = {
    val hostname = shardInfo.hostname
    var pool = concurrentServerMap.get(hostname);
    if(pool eq null) {
      val newPool = ClientPool(new LinkedBlockingQueue[PipelinedRedisClient](), new AtomicInteger())
      pool = concurrentServerMap.putIfAbsent(hostname, newPool);
      if(pool eq null) {
        pool = newPool
      }
    }
    while({
      val poolCount = pool.count.get()
      if(poolCount >= config.poolSize) {
        false
      }
      else if(pool.count.compareAndSet(poolCount, poolCount + 1)) {
        try {
          checkErrorCount(shardInfo)
          pool.available.offer(makeClient(hostname))
        } catch {
          case e: Throwable =>
            pool.count.decrementAndGet
            throw e
        }
        false
      }
      else {
        true
      }
    }) {}
    val client = pool.available.poll(poolTimeout.inMilliseconds, TimeUnit.MILLISECONDS)
    if (client eq null) {
      throw new TimeoutException("Unable to get redis connection to " + hostname)
    }
    client
  }

  def throwAway(hostname: String, client: PipelinedRedisClient) {
    try {
      client.shutdown()
    } catch {
      case e: Throwable =>
        log.warning(e, "Error discarding dead redis client: %s", e)
    }
    serverMap.get(hostname).foreach { _.count.decrementAndGet() }
  }

  def giveBack(hostname: String, client: PipelinedRedisClient) {
    if (client.alive) {
      serverMap(hostname).available.offer(client)
    }
  }

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
      if (timeout < Time.now) {
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

  def withClient[T](shardInfo: ShardInfo)(f: PipelinedRedisClient => T): T = {
    var client: PipelinedRedisClient = null
    val hostname = shardInfo.hostname
    try {
      client = Stats.timeMicros("redis-acquire-usec") { get(shardInfo) }
    } catch {
      case e: ShardBlackHoleException =>
        throw e
      case e =>
        countError(hostname)
        throw e
    }
    val r = try {
      f(client)
    } catch {
      case e: ClientRuntimeException =>
        log.error(e, "Redis client error: %s", e)
        countError(hostname)
        throwAway(hostname, client)
        throw e
      case e: Throwable =>
        log.error(e, "Non-redis error: %s", e)
        countError(hostname)
        throw e
    } finally {
      Stats.timeMicros("redis-release-usec") { giveBack(hostname, client) }
    }
    countNonError(hostname)
    r
  }

  def shutdown() {
      serverMap.foreach { case (hostname, pool) =>
        while (pool.available.size > 0) {
          try {
            pool.available.take().shutdown()
          } catch {
            case e: Throwable =>
              log.error(e, "Failed to shutdown client: %s", e)
          }
        }
      }
      serverMap.clear()
  }

  override def toString = {
    "<RedisPool: %s>".format(serverMap.map { case (hostname, pool) =>
      "%s=(%d available, %d total)".format(hostname, pool.available.size, pool.count.get())
    }.mkString(", "))
  }
}
