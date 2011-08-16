package com.twitter.haplocheirus.thrift.conversions

object TimelineGetRange {
  class RichThriftTimelineGetRange(get: thrift.TimelineGetRange) {
    def fromThrift = haplocheirus.TimelineGetRange(get.timeline_id, get.from_id, get.to_id, get.dedupe)
  }
  implicit def thriftTimelineGetRangeToRichTimelineGetRange(get: thrift.TimelineGetRange) =
    new RichThriftTimelineGetRange(get)
}
