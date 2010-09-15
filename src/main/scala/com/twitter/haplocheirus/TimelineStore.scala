package com.twitter.haplocheirus

import java.util.{List => JList}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.haplocheirus.thrift.conversions.TimelineSegment._
import thrift.TimelineStoreException


class TimelineStore(service: TimelineStoreService) extends thrift.TimelineStore.Iface {
  def append(entry: Array[Byte], timeline_prefix: String, timeline_ids: JList[java.lang.Long]) {
    service.append(entry, timeline_prefix, timeline_ids.toSeq)
  }

  def remove(entry: Array[Byte], timeline_prefix: String, timeline_ids: JList[java.lang.Long]) {
    service.remove(entry, timeline_prefix, timeline_ids.toSeq)
  }

  def filter(timeline_id: String, entries: JList[Array[Byte]], max_search: Int) = {
    service.filter(timeline_id, entries.toSeq, max_search).map(_.toJavaList).getOrElse {
      throw new TimelineStoreException("no timeline")
    }
  }

  def get(timeline_id: String, offset: Int, length: Int, dedupe: Boolean) = {
    service.get(timeline_id, offset, length, dedupe).map(_.toThrift).getOrElse {
      throw new TimelineStoreException("no timeline")
    }
  }

  def get_range(timeline_id: String, from_id: Long, to_id: Long, dedupe: Boolean) = {
    service.getRange(timeline_id, from_id, to_id, dedupe).map(_.toThrift).getOrElse {
      throw new TimelineStoreException("no timeline")
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

  def merge_indirect(dest_timeline_id: String, source_timeline_id: String) = {
    service.mergeIndirect(dest_timeline_id, source_timeline_id)
  }

  def unmerge_indirect(dest_timeline_id: String, source_timeline_id: String) = {
    service.unmergeIndirect(dest_timeline_id, source_timeline_id)
  }

  def delete_timeline(timeline_id: String) {
    service.deleteTimeline(timeline_id)
  }
}
