package com.twitter.haplocheirus

case class TimelineGet(timeline_id: String, offset: Int, length: Int, dedupe: Boolean)
