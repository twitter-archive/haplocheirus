namespace java com.twitter.haplocheirus.thrift
namespace ruby Haplocheirus


exception TimelineStoreException {
  1: string description
}

struct TimelineSegment {
  1: list<binary> entries;
  2: i32 size;
}

service TimelineStore {
  void append(1: binary entry, 2: string timeline_prefix, 3: list<i64> timeline_ids) throws(1: TimelineStoreException ex)
  void remove(1: binary entry, 2: string timeline_prefix, 3: list<i64> timeline_ids) throws(1: TimelineStoreException ex)

  /*
   * search a timeline for matching entries (based only on the initial i64 id) and return the
   * ones that were found. if max_search is -1, the entire timeline is searched; otherwise, only
   * the first N entries are searched. the returned entries will exactly match entries passed in --
   * that is, returned entries come from the passed-in set, not the actual timeline.
   */
  list<binary> filter(1: string timeline_id, 2: list<binary> entry, 3: i32 max_search) throws(1: TimelineStoreException ex)

  TimelineSegment get(1: string timeline_id, 2: i32 offset, 3: i32 length, 4: bool dedupe) throws(1: TimelineStoreException ex)
  TimelineSegment get_range(1: string timeline_id, 2: i64 from_id, 3: i64 to_id, 4: bool dedupe) throws(1: TimelineStoreException ex)
  void store(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)
  void merge(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)
  void unmerge(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)
  void delete_timeline(1: string timeline_id) throws(1: TimelineStoreException ex)
}
