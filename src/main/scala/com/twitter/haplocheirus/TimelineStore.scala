package com.twitter.haplocheirus

import java.util.{List => JList}
import com.twitter.gizzard.thrift.conversions.Sequences._


class TimelineStore(service: TimelineStoreService) extends thrift.TimelineStore.Iface {
  def append(entry: Array[Byte], timeline_ids: JList[String]) {
    service.append(entry, timeline_ids.toSeq)
  }

  def remove(entry: Array[Byte], timeline_ids: JList[String]) {
    service.remove(entry, timeline_ids.toSeq)
  }

  def filter(timeline_id: String, entries: JList[Array[Byte]]) = {
    service.filter(timeline_id, entries.toSeq).toJavaList
  }

  def get(timeline_id: String, offset: Int, length: Int, dedupe: Boolean) = {
    service.get(timeline_id, offset, length, dedupe).toJavaList
  }

  def get_since(timeline_id: String, from_id: Long, dedupe: Boolean) = {
    service.getSince(timeline_id, from_id, dedupe).toJavaList
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
