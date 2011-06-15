import com.twitter.haplocheirus.HaplocheirusConfig

import com.twitter.haplocheirus._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.util.TimeConversions._

new HaplocheirusConfig {
  val adminConfig = new AdminConfig {
    val httpPort = 7667
    val textPort = 7668
    val timeSeries = true
  }

  val server = new HaplocheirusServer with THsHaServer {
    timeout = 15.seconds
    idleTimeout = 300.seconds
    threadPool.minThreads = 100
  }

  val nameServer = new NameServer {
    mappingFunction = ByteSwapper
    val replicas = Seq(Memory)
  }

  jobInjector.timeout = 100.milliseconds
  jobInjector.idleTimeout = 60.seconds
  jobInjector.threadPool.minThreads = 1

  val redisConfig = new RedisConfig {
    val poolHealthTrackerConfig = new RedisPoolHealthTrackerConfig {
      val clientErrorLimit = 100
      val autoDisableErrorLimit = 200
      val autoDisableDuration = 60.seconds
    }

    val readPoolConfig = new RedisPoolConfig {
      val pipeline = 0  // for tests, force no pipeline, so we can see the results immediately.
      val timeoutMsec = 200
      val keysTimeoutMsec = 5000
      val expirationHours = 24
    }

    val writePoolConfig = new RedisPoolConfig {
      val pipeline = 0  // for tests, force no pipeline, so we can see the results immediately.
      val timeoutMsec = 200
      val keysTimeoutMsec = 5000
      val expirationHours = 24
    }

    val slowPoolConfig = new RedisPoolConfig {
      val pipeline = 0  // for tests, force no pipeline, so we can see the results immediately.
      val timeoutMsec = 200
      val keysTimeoutMsec = 5000
      val expirationHours = 24
    }

    val rangeQueryPageSize = 20
  }

  val timelineTrimConfig = new TimelineTrimConfig {
    val bounds = Map(
      "default" -> new TimelineTrimBounds {
        val lower = 800
        val upper = 850
      }
    )
  }

  val jobQueues = Map(
    Priority.Copy.id -> new Scheduler {
      val name = "copy"
      val schedulerType = new MemoryScheduler
      threads = 1
      errorLimit = 25
      errorRetryDelay = 900.seconds
      badJobQueue = new JsonJobLogger { name = "bad_jobs" }
    },

    Priority.Write.id -> new Scheduler {
      val name = "write"
      val schedulerType = new MemoryScheduler
      threads = 1
      errorLimit = 25
      errorRetryDelay = 900.seconds
      badJobQueue = new JsonJobLogger { name = "bad_jobs" }
    },

    Priority.MultiPush.id -> new Scheduler {
      val name = "multipush"
      val schedulerType = new MemoryScheduler
      threads = 1
      errorLimit = 25
      errorRetryDelay = 900.seconds
      badJobQueue = new JsonJobLogger { name = "bad_jobs" }
    }
  )

  logging = new LogConfigString("""
                                level = "fatal"
                                console = true
                                throttle_period_msec = 60000
                                throttle_rate = 10
                                """)
}
