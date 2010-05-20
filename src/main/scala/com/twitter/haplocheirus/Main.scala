package com.twitter.haplocheirus

import java.util.{List => JList}
import java.util.concurrent.CountDownLatch
import com.twitter.gizzard.thrift.{GizzardServices, TSelectorServer}
import com.twitter.ostrich.{BackgroundProcess, Service, ServiceTracker, Stats}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Configgy, ConfigMap, RuntimeEnvironment}
import net.lag.logging.Logger
import org.jredis._
import org.jredis.ri.alphazero.JRedisClient


class TimelineStore extends thrift.TimelineStore.Iface {
  def append(entry: Array[Byte], timeline_ids: JList[String]) { }
  def remove(entry: Array[Byte], timeline_ids: JList[String]) { }
  def get(timeline_id: String, offset: Int, length: Int) = null
  def get_range(timeline_id: String, from_id: Long, to_id: Long) = null
  def store(timeline_id: String, entries: JList[Array[Byte]]) { }
  def merge(timeline_id: String, entries: JList[Array[Byte]]) { }
  def unmerge(timeline_id: String, entries: JList[Array[Byte]]) { }

    // ...
    /*
        val redis = new JRedisClient()

        var currentTimeline = 0
        var statusId = 1L
        val buffer = ByteBuffer.wrap(new Array[Byte](16))
        buffer.order(ByteOrder.LITTLE_ENDIAN)

          buffer.clear()
          buffer.putLong(statusId)
          buffer.putLong(0)
          val key = "timeline:" + currentTimeline
          try {
    //        redis.zadd(key, statusId.toDouble, buffer.array)
            redis.rpush(key, buffer.array)
          } catch {
            case e: RedisException =>
              println("filled up memory. statuses=%d".format(statusId))
              e.printStackTrace()
              System.exit(1)
          }
          */

}

object Main extends Service {
  val log = Logger.get(getClass.getName)
  var thriftServer: TSelectorServer = null
  var gizzardServices: GizzardServices[HaplocheirusShard] = null
  private val deathSwitch = new CountDownLatch(1)

  def main(args: Array[String]) {
    val runtime = new RuntimeEnvironment(getClass)
    runtime.load(args)
    val config = Configgy.config
    ServiceTracker.register(this)
    ServiceTracker.startAdmin(config, runtime)

    log.info("Starting haplocheirus!")
    startThrift(config)

    // ROBEY
    val client = new RedisClient("localhost")
    println("here goes:")
    println(client.rpush("timeline:0", "hello".getBytes))

    deathSwitch.await
  }

  def shutdown() {
    log.info("Shutting down!")
    stopThrift()
    deathSwitch.countDown()
  }

  def quiesce() {
    shutdown()
  }

  def startThrift(config: ConfigMap) {
    try {
//      gizzardServices = new GizzardServices(config.configMap("gizzard_services"),
//                                            flock.edges.nameServer,
//                                            flock.edges.copyFactory,
//                                            flock.edges.schedule,
//                                            Priority.Medium.id)

      val processor = new thrift.TimelineStore.Processor(new TimelineStore())
      thriftServer = TSelectorServer("timelines", config("server_port").toInt,
                                     config.configMap("gizzard_services"), processor)
      thriftServer.serve()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        log.error(e, "Unexpected exception: %s", e.getMessage)
        System.exit(0)
    }
  }

  def stopThrift() {
    log.info("Thrift servers shutting down...")
    thriftServer.shutdown()
    thriftServer = null
  }
}