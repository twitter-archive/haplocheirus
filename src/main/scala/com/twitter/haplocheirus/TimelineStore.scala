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

  def get(timeline_id: String, offset: Int, length: Int) = {
    service.get(timeline_id, offset, length).toJavaList
  }

  def get_range(timeline_id: String, from_id: Long, to_id: Long) = null
  def store(timeline_id: String, entries: JList[Array[Byte]]) { }
  def merge(timeline_id: String, entries: JList[Array[Byte]]) { }
  def unmerge(timeline_id: String, entries: JList[Array[Byte]]) { }
  def delete_timeline(timeline_id: String) { }
}
