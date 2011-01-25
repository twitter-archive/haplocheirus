import com.twitter.haplocheirus._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.query.QueryClass
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

  jobInjector.timeout = 100.milliseconds
  jobInjector.idleTimeout = 60.seconds
  jobInjector.threadPool.minThreads = 30
  jobInjector.threadPool.maxThreads = 30

  class ProductionNameServerReplica(host: String) extends Mysql {
    val connection = new Connection {
      val username = "twitteradmin2"
      val password = "xs8hLQTE"
      val hostnames = Seq(host)
      val database  = "haplo_production"
    }

    queryEvaluator = new QueryEvaluator {
      autoDisable = new AutoDisablingQueryEvaluator {
        val errorCount = 100
        val interval = 60.seconds
      }

      database.pool = new ApachePoolingDatabase {
        maxWait          = 100.millis
        testIdle         = 1000.millis
        testOnBorrow     = false
        minEvictableIdle = (-1).millis
        sizeMin          = 1
        sizeMax          = 1
        maxWait          = 100.second
      }

      database.timeout = new TimingOutDatabase {
        poolSize   = 10
        queueSize  = 10000
        open       = 500.millis
      }

      query.timeouts = Map(
        QueryClass.Select  -> QueryTimeout(10.seconds),
        QueryClass.Execute -> QueryTimeout(1.second)
      )
    }
  }

  val nameServer = new NameServer {
    mappingFunction = Fnv1a64
    val replicas = Seq(new ProductionNameServerReplica("smf1-aca-27-sr1"))
    // TODO(benjy): Set these up in production, then use this instead of the temporary
    // nameserver above.
    //val replicas = Seq(new ProductionNameServerReplica("smf1-ach-15-sr1"),
    //                   new ProductionNameServerReplica("smf1-acb-23-sr1"))
  }

  val redisConfig = new RedisConfig {
    val readPoolConfig = new RedisPoolConfig {
      val poolSize = 1
      val poolTimeoutMsec = 5000
      val pipeline = 100
      val timeoutMsec = 200
      val keysTimeoutMsec = 5000
      val expirationHours = 24 * 21
    }

    val writePoolConfig = new RedisPoolConfig {
      val poolSize = 1
      val poolTimeoutMsec = 5000
      val pipeline = 100
      val timeoutMsec = 200
      val keysTimeoutMsec = 5000
      val expirationHours = 24 * 21
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

  val replicationFuture = new Future {
    poolSize = 100
    maxPoolSize = 100
    keepAlive = 5.seconds
    timeout = 6.seconds
  }

  val readFuture = new Future {
    poolSize = 100
    maxPoolSize = 100
    keepAlive = 5.seconds
    timeout = 6.seconds
  }

  val jobQueues = Map(
    Priority.Copy.id -> new Scheduler {
      val name = "copy"
      val schedulerType = new KestrelScheduler {
        path = "/var/spool/kestrel"
      }
      threads = 10
      errorLimit = 100
      errorRetryDelay = 900.seconds
      badJobQueue = new JsonJobLogger { name = "bad_jobs" }
    },

    Priority.Write.id -> new Scheduler {
      val name = "write"
      val schedulerType = new KestrelScheduler {
        path = "/var/spool/kestrel"
      }
      threads = 32
      errorLimit = 100
      errorRetryDelay = 60.seconds
      badJobQueue = new JsonJobLogger { name = "bad_jobs" }
    },

    Priority.MultiPush.id -> new Scheduler {
      val name = "multipush"
      val schedulerType = new MemoryScheduler
      threads = 32
      errorLimit = 100
      errorRetryDelay = 60.seconds
      badJobQueue = new JsonJobLogger { name = "bad_jobs" }
    }
  )

  logging = new LogConfigString("""
                                level = "info"
                                filename = "/var/log/haplocheirus/production.log"
                                throttle_period_msec = 60000
                                truncate_stack_traces = 0
                                throttle_rate = 10
                                roll = "hourly"
                                handle_sighup = true

                                stats {
                                  node = "stats"
                                  use_parents = false
                                  level = "info"
                                  scribe_category = "ostrich"
                                  scribe_server = "localhost"
                                  scribe_max_packet_size = 100
                                }

                                exception {
                                  filename = "/var/log/haplocheirus/exception.log"
                                  roll = "hourly"
                                  format = "exception_json"
                                  handle_sighup = true
                                }
                                """)
}
