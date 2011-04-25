package com.twitter.haplocheirus

import scala.util.Random

object Stats extends StatsProvider {}

trait StatsProvider {
  var sampleRate = 10
  private val random = new Random()

  def timeMicros[T](name: String)(f: => T): T = {
    if (random.nextInt(sampleRate) == 0) {
      com.twitter.ostrich.Stats.timeMicros(name)(f)
    } else {
      f
    }
  }

  def addTiming(name: String, duration: Int) {
    if (random.nextInt(sampleRate) == 0) {
      com.twitter.ostrich.Stats.addTiming(name, duration)
    }
  }

  def time[T](k: String)(f: => T): T = com.twitter.ostrich.Stats.time(k)(f)

  def incr(name: String, count: Int): Long = com.twitter.ostrich.Stats.incr(name, count)

  def incr(name: String): Long = com.twitter.ostrich.Stats.incr(name)

  def getTiming(name: String) = com.twitter.ostrich.Stats.getTiming(name)
}
