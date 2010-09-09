package com.twitter.haplocheirus

import java.util.{List => JList}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.haplocheirus.thrift.conversions.TimelineSegment._


class TimelineStore(service: TimelineStoreService) extends thrift.TimelineStore.Iface {
  def append(entry: Array[Byte], timeline_prefix: String, timeline_ids: JList[java.lang.Long]) {
    service.append(entry, timeline_prefix, timeline_ids.toSeq)
  }

  def remove(entry: Array[Byte], timeline_prefix: String, timeline_ids: JList[java.lang.Long]) {
    service.remove(entry, timeline_prefix, timeline_ids.toSeq)
  }

  def filter(timeline_id: String, entries: JList[Array[Byte]], max_search: Int) = {
    service.filter(timeline_id, entries.toSeq, max_search) match {
      case None =>
        List[Array[Byte]]().toJavaList
      case Some(x) =>
        x.toJavaList
    }
  }

  def get(timeline_id: String, offset: Int, length: Int, dedupe: Boolean) = {
    service.get(timeline_id, offset, length, dedupe) match {
      case None =>
        new TimelineSegment(Nil, 0).toThrift
      case Some(x) =>
        x.toThrift
    }
  }

  def get_range(timeline_id: String, from_id: Long, to_id: Long, dedupe: Boolean) = {
    service.getRange(timeline_id, from_id, to_id, dedupe) match {
      case None =>
        new TimelineSegment(Nil, 0).toThrift
      case Some(x) =>
        x.toThrift
    }
  }

  def store(timeline_id: String, entries: JList[Array[Byte]]) {
    service.store(timeline_id, entries.toSeq)
  }

  def merge(timeline_id: String, entries: JList[Array[Byte]]) {
    service.merge(timeline_id, entries.toSeq)
  }

  def unmerge(timeline_id: String, entries: JList[Array[Byte]]) {
    service.unmerge(timeline_id, entries.toSeq)
  }

  def delete_timeline(timeline_id: String) {
    service.deleteTimeline(timeline_id)
  }
}
