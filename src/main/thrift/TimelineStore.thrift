namespace java com.twitter.haplocheirus.thrift
namespace ruby Haplocheirus


exception TimelineStoreException {
  1: string description
}

service TimelineStore {
  void append(1: binary entry, 2: list<string> timeline_ids) throws(1: TimelineStoreException ex)
  void remove(1: binary entry, 2: list<string> timeline_ids) throws(1: TimelineStoreException ex)
  list<binary> filter(1: string timeline_id, 2: list<binary> entry) throws(1: TimelineStoreException ex)
  list<binary> get(1: string timeline_id, 2: i32 offset, 3: i32 length, 4: bool dedupe) throws(1: TimelineStoreException ex)
  list<binary> get_range(1: string timeline_id, 2: i64 from_id, 3: i64 to_id, 4: bool dedupe) throws(1: TimelineStoreException ex)
  void store(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)
  void merge(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)
  void unmerge(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)
  void delete_timeline(1: string timeline_id) throws(1: TimelineStoreException ex)
}
