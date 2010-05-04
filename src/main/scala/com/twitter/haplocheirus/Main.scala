package com.twitter.haplocheirus

import com.twitter.ostrich.{BackgroundProcess, Service, ServiceTracker, Stats}
import net.lag.configgy.{Configgy, RuntimeEnvironment}
import net.lag.logging.Logger


object Main extends Service {
  val log = Logger.get(getClass.getName)

  def main(args: Array[String]) {
    val runtime = new RuntimeEnvironment(getClass)
    runtime.load(args)
    val config = Configgy.config
    ServiceTracker.register(this)
    ServiceTracker.startAdmin(config, runtime)

    log.info("Starting haplocheirus!")
    BackgroundProcess.spawnDaemon("main") {
      while (true) {
        Thread.sleep(2000)
        Stats.incr("sheep")
      }
    }
  }

  def shutdown() {
    log.info("Shutting down!")
    System.exit(0)
  }

  def quiesce() {
    shutdown()
  }
}