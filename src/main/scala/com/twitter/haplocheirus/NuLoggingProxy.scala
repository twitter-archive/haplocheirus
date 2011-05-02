package com.twitter.haplocheirus

import scala.reflect.Manifest
import com.twitter.gizzard.proxy.Proxy
import com.twitter.ostrich.{Stats, StatsProvider}


object NuLoggingProxy {
  def apply[T <: AnyRef](stats: StatsProvider, name: String, obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      stats.incr("operation-" + name + "-" + method.name)
      stats.timeMicros("operation-" + name + "-" + method.name + "-usec") {
        method()
      }
    }
  }
}
