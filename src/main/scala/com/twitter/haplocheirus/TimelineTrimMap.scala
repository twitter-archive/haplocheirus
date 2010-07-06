package com.twitter.haplocheirus

import scala.collection.mutable.ListBuffer
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


class TimelineTrimMap(config: ConfigMap) {
  private val log = Logger.get(getClass.getName)

  case class TrimEntry(prefix: String, lowerBound: Int, upperBound: Int)

  @volatile var trimMap: List[TrimEntry] = null
  @volatile var defaultTrim: TrimEntry = null

  config.subscribe(reload(_))
  reload(Some(config))

  def reload(config: Option[ConfigMap]) {
    val newMap = new ListBuffer[TrimEntry]()
    var newDefaultTrim: Option[TrimEntry] = None
    try {
      config.map { c =>
        c.keys.foreach { prefix =>
          val bounds = c.getList(prefix).map { _.toInt }.toList
          val trimEntry = TrimEntry(prefix, bounds(0), bounds(1))
          if (prefix == "default") {
            newDefaultTrim = Some(trimEntry)
          } else {
            newMap += trimEntry
          }
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
  }

  def getBounds(timeline: String): (Int, Int) = {
    val entry = trimMap.find { timeline startsWith _.prefix }.getOrElse(defaultTrim)
    (entry.lowerBound, entry.upperBound)
  }
}
