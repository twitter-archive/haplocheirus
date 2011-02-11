package com.twitter.haplocheirus

import java.util.concurrent.{LinkedBlockingQueue, TimeoutException, TimeUnit, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import org.jredis.ClientRuntimeException


class RedisPool(name: String, config: ConfigMap) {
  case class ClientPool(available: LinkedBlockingQueue[PipelinedRedisClient], val count: AtomicInteger)

  val log = Logger(getClass.getName)

  val poolSize = config("pool_size").toInt
  val poolTimeout = config("pool_timeout_msec").toInt.milliseconds
  val concurrentServerMap = new ConcurrentHashMap[String, ClientPool]
  val serverMap = scala.collection.jcl.Map(concurrentServerMap)

  Stats.makeGauge("redis-pool-" + name) {
    serverMap.values.foldLeft(0) { _ + _.available.size }
  }

  def makeClient(hostname: String) = {
    val pipelineSize = config("pipeline").toInt
    val timeout = config("timeout_msec").toInt.milliseconds
    val keysTimeout = config("keys_timeout_msec").toInt.milliseconds
    val expiration = config("expiration_hours").toInt.hours
    new PipelinedRedisClient(hostname, pipelineSize, timeout, keysTimeout, expiration)
  }

  def get(hostname: String): PipelinedRedisClient = {
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
      if(poolCount >= poolSize) {
        false
      }
      else if(pool.count.compareAndSet(poolCount, poolCount + 1)) {
        pool.available.offer(makeClient(hostname))
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
