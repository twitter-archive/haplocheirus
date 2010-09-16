package com.twitter.haplocheirus

import java.util.concurrent.CountDownLatch
import com.twitter.gizzard.proxy.ExceptionHandlingProxy
import com.twitter.gizzard.thrift.GizzardServices
import com.twitter.ostrich.{BackgroundProcess, Service, ServiceTracker, Stats}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Configgy, ConfigMap, RuntimeEnvironment}
import net.lag.logging.Logger
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket

object Main extends Service {
  val log = Logger.get(getClass.getName)
  var thriftServer: TThreadPoolServer = null
  var gizzardServices: GizzardServices[HaplocheirusShard] = null
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
    service.shutdown()
    stopThrift()
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
                                            Priority.Copy.id)
      gizzardServices.start()

      val processor = new thrift.TimelineStore.Processor(
        TimelineStoreExceptionWrappingProxy(
          NuLoggingProxy[thrift.TimelineStore.Iface](Stats, "timelines", new TimelineStore(service))
        )
      )
      thriftServer = new TThreadPoolServer(processor, new TServerSocket(config("server_port").toInt))
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
    // thrift server doesn't correctly die. :(
    val t = new Thread() {
      override def run() {
        thriftServer.stop()
        thriftServer = null
      }
    }
    t.setDaemon(true)
    t.start()
    Thread.sleep(1000)
  }
}
