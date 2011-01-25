package com.twitter.haplocheirus

import scala.collection.mutable.ListBuffer
import net.lag.logging.Logger


class TimelineTrimMap(config: TimelineTrimConfig) {
  private val log = Logger.get(getClass.getName)

  case class TrimEntry(prefix: String, lowerBound: Int, upperBound: Int)

  @volatile var trimMap: List[TrimEntry] = null
  @volatile var defaultTrim: TrimEntry = null

  val newMap = new ListBuffer[TrimEntry]()
  var newDefaultTrim: Option[TrimEntry] = None
  try {
    config.bounds.foreach { case (prefix, bounds) =>
      val trimEntry = TrimEntry(prefix, bounds.lower, bounds.upper)
      if (prefix == "default") {
        newDefaultTrim = Some(trimEntry)
      } else {
        newMap += trimEntry
      }
    }
  } catch {
    case e: Throwable =>
      log.error(e, "Unable to parse timeline trim map: %s", e.toString)
    throw e
  }
  // atomic:
  trimMap = newMap.toList
  newDefaultTrim.foreach { defaultTrim = _ }


  def getBounds(timeline: String): (Int, Int) = {
    val entry = trimMap.find { timeline startsWith _.prefix }.getOrElse(defaultTrim)
    (entry.lowerBound, entry.upperBound)
  }
}
