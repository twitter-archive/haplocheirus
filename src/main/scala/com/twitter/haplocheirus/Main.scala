package com.twitter.haplocheirus

import java.util.concurrent.CountDownLatch
import com.twitter.gizzard.proxy.ExceptionHandlingProxy
import com.twitter.gizzard.scheduler.JsonJob
import com.twitter.gizzard.thrift.GizzardServices
import com.twitter.ostrich.{BackgroundProcess, Service, ServiceTracker, Stats}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Configgy, ConfigMap, RuntimeEnvironment}
import net.lag.logging.Logger
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import thrift.BetterTThreadPoolServer

object Main extends Service {
  val log = Logger.get(getClass.getName)
  var thriftServer: BetterTThreadPoolServer = null
  var gizzardServices: GizzardServices[HaplocheirusShard, JsonJob] = null
  var service: TimelineStoreService = null

  private val deathSwitch = new CountDownLatch(1)

  object TimelineStoreExceptionWrappingProxy extends ExceptionHandlingProxy({ e =>
    throw new thrift.TimelineStoreException(e.toString)
  })

  def main(args: Array[String]) {
    val runtime = new RuntimeEnvironment(getClass)
    runtime.load(args)
    val config = Configgy.config
    ServiceTracker.register(this)
    ServiceTracker.startAdmin(config, runtime)

    log.info("Starting haplocheirus!")
    startThrift(config)

    deathSwitch.await
  }

  def shutdown() {
    log.info("Shutting down!")
    // stop thrift first, so new work stops arriving.
    stopThrift()
    service.shutdown()
    deathSwitch.countDown()
    log.info("Goodbye!")
    System.exit(0)
  }

  def quiesce() {
    shutdown()
  }

  def startThrift(config: ConfigMap) {
    try {
      service = Haplocheirus(config)
      gizzardServices = new GizzardServices(config.configMap("gizzard_services"),
                                            service.nameServer,
                                            service.copyFactory,
                                            service.scheduler,
                                            service.scheduler(Priority.Copy.id))
      gizzardServices.start()

      val processor = new thrift.TimelineStore.Processor(
        TimelineStoreExceptionWrappingProxy(
          NuLoggingProxy[thrift.TimelineStore.Iface](Stats, "timelines", new TimelineStore(service))
        )
      )
      thriftServer = BetterTThreadPoolServer("haplocheirus", config("server_port").toInt,
                                             config("timeline_store_service.idle_timeout_sec").toInt * 1000,
                                             processor)
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
    gizzardServices.shutdown()
    gizzardServices = null
    thriftServer.stop()
    thriftServer = null
  }
}
