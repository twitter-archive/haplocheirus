package com.twitter.haplocheirus

import java.lang.System
import java.nio.ByteBuffer
import java.util.{List => JList}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.haplocheirus.thrift.conversions.TimelineSegment._
import thrift.TimelineStoreException


class TimelineStore(service: TimelineStoreService) extends thrift.TimelineStore.Iface {
  def getArray(entry: ByteBuffer) = {
    val res = new Array[Byte](entry.remaining)
    System.arraycopy(entry.array, entry.position, res, 0, entry.remaining)
    res
  }

  def append(entry: ByteBuffer, timeline_prefix: String, timeline_ids: JList[java.lang.Long]) {
    service.append(getArray(entry), timeline_prefix, timeline_ids.toSeq)
  }

  def remove(entry: ByteBuffer, timeline_prefix: String, timeline_ids: JList[java.lang.Long]) {
    service.remove(getArray(entry), timeline_prefix, timeline_ids.toSeq)
  }

  def filter(timeline_id: String, entries: JList[ByteBuffer], max_search: Int) = {
    service.filter(timeline_id, entries.toSeq.map(getArray(_)), max_search).map(_.map(ByteBuffer.wrap(_)).toJavaList).getOrElse {
      throw new TimelineStoreException("no timeline")
    }
  }

  def filter2(timeline_id: String, entries: JList[java.lang.Long], max_search: Int) = {
    service.filter2(timeline_id, entries.toSeq, max_search).map(_.map(ByteBuffer.wrap(_)).toJavaList).getOrElse {
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

  def store(timeline_id: String, entries: JList[ByteBuffer]) {
    service.store(timeline_id, entries.toSeq.map(getArray(_)))
  }

  def merge(timeline_id: String, entries: JList[ByteBuffer]) {
    service.merge(timeline_id, entries.toSeq.map(getArray(_)))
  }

  def unmerge(timeline_id: String, entries: JList[ByteBuffer]) {
    service.unmerge(timeline_id, entries.toSeq.map(getArray(_)))
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
