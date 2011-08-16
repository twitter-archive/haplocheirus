package com.twitter.haplocheirus


case class TimelineSegment(entries: Seq[Array[Byte]], size: Int) {
  var state = thrift.TimelineSegmentState.HIT
}
