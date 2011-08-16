package com.twitter.haplocheirus.thrift.conversions

import java.nio.ByteBuffer
import com.twitter.gizzard.thrift.conversions.Sequences._


object TimelineSegment {
  class RichTimelineSegment(segment: haplocheirus.TimelineSegment) {
    def toThrift = {
      val rv = new thrift.TimelineSegment()
      rv.setEntries((segment.entries map (ByteBuffer.wrap(_))).toJavaList)
      rv.setSize(segment.size)
      rv.state = segment.state
      rv
    }
  }
  implicit def timelineSegmentToRichTimelineSegment(segment: haplocheirus.TimelineSegment) =
    new RichTimelineSegment(segment)

  class RichThriftTimelineSegment(segment: thrift.TimelineSegment) {
    def fromThrift = haplocheirus.TimelineSegment(segment.entries.toSeq map (_.array()), segment.entries.size)
  }
  implicit def thriftTimelineSegmentToRichThriftTimelineSegment(segment: thrift.TimelineSegment) =
    new RichThriftTimelineSegment(segment)
}
