package com.twitter.haplocheirus.thrift.conversions

object TimelineGet {
  class RichThriftTimelineGet(get: thrift.TimelineGet) {
    def fromThrift = haplocheirus.TimelineGet(get.timeline_id, get.offset, get.length, get.dedupe)
  }
  implicit def thriftTimelineGetToRichTimelineGet(get: thrift.TimelineGet) =
    new RichThriftTimelineGet(get)
}
