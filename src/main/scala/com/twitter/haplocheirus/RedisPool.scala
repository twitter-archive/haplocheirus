package com.twitter.haplocheirus

import java.util.concurrent.{LinkedBlockingQueue, TimeoutException, TimeUnit}
import scala.collection.mutable
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import org.jredis.ClientRuntimeException


class RedisPool(name: String, config: RedisPoolConfig) {
  case class ClientPool(available: LinkedBlockingQueue[PipelinedRedisClient], var count: Int)

  val log = Logger(getClass.getName)

  val poolTimeout = config.poolTimeoutMsec.toInt.milliseconds
  val serverMap = new mutable.HashMap[String, ClientPool]

  Stats.makeGauge("redis-pool-" + name) {
    synchronized {
      serverMap.values.foldLeft(0) { _ + _.available.size }
    }
  }

  def makeClient(hostname: String) = {
    val timeout = config.timeoutMsec.milliseconds
    val keysTimeout = config.keysTimeoutMsec.milliseconds
    val expiration = config.expirationHours.hours
    new PipelinedRedisClient(hostname, config.pipeline, timeout, keysTimeout, expiration)
  }

  def get(hostname: String): PipelinedRedisClient = {
    val pool = synchronized {
      val pool = serverMap.getOrElseUpdate(hostname, {
        val queue = new LinkedBlockingQueue[PipelinedRedisClient]()
        ClientPool(queue, 0)
      })
      if (pool.count < config.poolSize) {
        pool.available.offer(makeClient(hostname))
        pool.count += 1
      }
      pool
    }
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
    synchronized {
      serverMap.get(hostname).foreach { _.count -= 1 }
    }
  }

  def giveBack(hostname: String, client: PipelinedRedisClient) {
    if (client.alive) {
      synchronized {
        serverMap(hostname).available.offer(client)
      }
    }
  }

  def withClient[T](hostname: String)(f: PipelinedRedisClient => T): T = {
    val client = Stats.timeMicros("redis-acquire-usec") { get(hostname) }
    try {
      f(client)
    } catch {
      case e: ClientRuntimeException =>
        log.error(e, "Redis client error: %s", e)
        throwAway(hostname, client)
        throw e
    } finally {
      Stats.timeMicros("redis-release-usec") { giveBack(hostname, client) }
    }
  }

  def shutdown() {
    synchronized {
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
  }

  override def toString = synchronized {
    "<RedisPool: %s>".format(serverMap.map { case (hostname, pool) =>
      "%s=(%d available, %d total)".format(hostname, pool.available.size, pool.count)
    }.mkString(", "))
  }
}
