package com.twitter.haplocheirus

import net.lag.configgy.{Config, Configgy}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object TimelineTrimMapSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val configString = """
    user_timeline = [ 1200, 1250 ]
    home_timeline = [ 3200, 3400 ]
    default = [ 800, 850 ]
  """
  val trimConfig = Config.fromString(configString)

  "TimelineTrimMap" should {
    var timelineTrimMap: TimelineTrimMap = null

    doBefore {
      timelineTrimMap = new TimelineTrimMap(trimConfig)
    }

    "find timeline-specific bounds" in {
      timelineTrimMap.getBounds("user_timeline:30") mustEqual (1200, 1250)
      timelineTrimMap.getBounds("user_timeline:99") mustEqual (1200, 1250)
      timelineTrimMap.getBounds("user_timeline_500") mustEqual (1200, 1250)
      timelineTrimMap.getBounds("user_timeline") mustEqual (1200, 1250)
      timelineTrimMap.getBounds("home_timeline:500") mustEqual (3200, 3400)
      timelineTrimMap.getBounds("home_timeline") mustEqual (3200, 3400)
    }

    "find default bounds" in {
      timelineTrimMap.getBounds("foobar") mustEqual (800, 850)
      timelineTrimMap.getBounds("") mustEqual (800, 850)
      timelineTrimMap.getBounds("user_timelinx") mustEqual (800, 850)
    }
  }
}
