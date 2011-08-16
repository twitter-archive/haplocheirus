package com.twitter.haplocheirus

case class TimelineGetRange(timeline_id: String, from_id: Long, to_id: Long, dedupe: Boolean)
