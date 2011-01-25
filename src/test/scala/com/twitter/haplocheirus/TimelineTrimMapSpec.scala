package com.twitter.haplocheirus

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object TimelineTrimMapSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val trimConfig =  new TimelineTrimConfig {
      val bounds = Map(
        "user_timeline" -> new TimelineTrimBounds { val lower = 1200; val upper = 1250 },
        "home_timeline" -> new TimelineTrimBounds { val lower = 3200; val upper = 3400 },
        "default" -> new TimelineTrimBounds { val lower = 800; val upper = 850 }
      )
    }

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
