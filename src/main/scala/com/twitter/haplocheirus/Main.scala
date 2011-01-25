package com.twitter.haplocheirus

import java.io.File
import java.util.concurrent.CountDownLatch
import com.twitter.gizzard.scheduler.JsonJob
import com.twitter.gizzard.thrift.TThreadServer
import com.twitter.ostrich.{BackgroundProcess, JsonStatsLogger, Service, ServiceTracker, Stats}
import com.twitter.util.Eval
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Config => CConfig, RuntimeEnvironment}
import net.lag.logging.Logger
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket

object Main extends Service {
  val log = Logger.get(getClass.getName)
  var service: Haplocheirus = null
  var config: HaplocheirusConfig = null
  var statsLogger: JsonStatsLogger = null


  def main(args: Array[String]) {
    try {
      config  = Eval[HaplocheirusConfig](args.map(new File(_)): _*)
      service = new Haplocheirus(config)

      start()

      println("Running.")
    } catch {
      case e => {
        println("Exception in initialization: ")
        Logger.get("").fatal(e, "Exception in initialization.")
        e.printStackTrace
        shutdown()
      }
    }
  }

  def start() {
    ServiceTracker.register(this)
    val adminConfig = new CConfig
    adminConfig.setInt("admin_http_port", config.adminConfig.httpPort)
    adminConfig.setInt("admin_text_port", config.adminConfig.textPort)
    adminConfig.setBool("admin_timeseries", config.adminConfig.timeSeries)
    // TODO(benjy): Add other ServiceTracker config params as needed.
    ServiceTracker.startAdmin(adminConfig, new RuntimeEnvironment(this.getClass))

    log.info("Starting haplocheirus!")

    statsLogger = new JsonStatsLogger(Logger.get("stats"), 1.minute, Some("haplocheirus"))
    statsLogger.start()

    service.start()
  }

  def shutdown() {
    log.info("Shutting down!")
    if (service ne null) service.shutdown()
    service = null
    ServiceTracker.stopAdmin()
    log.info("Goodbye!")
  }

  def quiesce() {
    log.info("Quiescing!")
    if (service ne null) service.shutdown(true)
    service = null
    ServiceTracker.stopAdmin()
  }
}
