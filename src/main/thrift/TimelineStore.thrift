namespace java com.twitter.haplocheirus.thrift
namespace rb Haplocheirus


exception TimelineStoreException {
  1: string description
}

struct TimelineSegment {
  /*
   * Timeline entries are opaque binary data, with the first 20 bytes defined by the server:
   *
   *   1. The first i64 (little-endian) is a sort key. Two entries with the same sort key will be
   *      considered equivalent (without looking at the rest of the entry) and only one will be
   *      returned.
   *   2. The second i64 (little-endian) is a secondary key. If bit 31 is set in the flags, this
   *      key will be used as a secondary search key for 'filter', and if "dedupe=true" is passed
   *      to 'get' or 'get_range', entries will be deduped by the secondary key.
   *   3. The next i32 (little-endian) is a set of flags. The upper 8 bits are reserved by the
   *      server; the lower 24 bits may be used by the client.
   *        bit 31 - secondary key is active (should be used to sort/dedupe)
   *        bit 30 - reserved
   *        bit 29 - reserved
   *        bit 28 - reserved
   *        bit 27 - reserved
   *        bit 26 - reserved
   *        bit 25 - reserved
   *        bit 24 - reserved
   *
   * Haplocheirus will make no assumptions about the content of each timeline entry after the first
   * 20 bytes. Entries can vary by size within the same timeline.
   */
  1: list<binary> entries;

  /*
   * The actual full length of the timeline as stored in redis.
   */
  2: i32 size;
  3: optional bool hit = 1;
}

struct TimelineGet {
  1: string timeline_id;
  2: i32 offset;
  3: i32 length;
  4: optional bool dedupe = 0;
}

struct TimelineGetRange {
  1: string timeline_id;
  2: i64 from_id;
  3: i64 to_id;
  4: optional bool dedupe = 0;
}

service TimelineStore {
  /*
   * Write an entry into the timeline names given. Appends will silently do nothing if a timeline
   * has not been created using "store". The timeline names are given by turning the given
   * timeline_ids into base-10 strings and appending them directly to the prefix, so (for example)
   * the call "append(entry, 'home_timeline:', [ 10, 20 ])" will append to the timelines named
   * "home_timeline:10" and "home_timeline:20".
   */
  void append(1: binary entry, 2: string timeline_prefix, 3: list<i64> timeline_ids) throws(1: TimelineStoreException ex)

  /*
   * Remove an entry from timelines. This is exactly the opposite of "append".
   */
  void remove(1: binary entry, 2: string timeline_prefix, 3: list<i64> timeline_ids) throws(1: TimelineStoreException ex)

  /*
   * Search a timeline for matching entries (based only on the initial i64 id) and return the
   * ones that were found. If max_search is -1, the entire timeline is searched; otherwise, only
   * the first N entries are searched. The returned entries will exactly match entries passed in --
   * that is, returned entries come from the passed-in set, not the actual timeline.
   * Throw "TimelineStoreException" if there's no such timeline in cache.
   */
  list<binary> filter(1: string timeline_id, 2: list<binary> entries, 3: i32 max_search) throws(1: TimelineStoreException ex)
  list<binary> filter2(1: string timeline_id, 2: list<i64> id, 3: i32 max_search) throws(1: TimelineStoreException ex)

  /*
   * Fetch a span of entries from a timeline. The offset & length are counted from most recent to
   * oldest, so an offset of 0 is the newest entry. If "dedupe" is true, only entries with unique
   * dedupe keys (the 2nd i64 of a timeline entry) will be returned.
   * Throw "TimelineStoreException" if there's no such timeline in cache.
   */
  TimelineSegment get(1: string timeline_id, 2: i32 offset, 3: i32 length, 4: bool dedupe) throws(1: TimelineStoreException ex)

  /*
   * Unlike the normal "get" call, this call will set hit=false for missing timelines instead of
   * throwing a "TimlineStoreException".
   */
  list<TimelineSegment> get_multi(1: list<TimelineGet> gets)

  /*
   * Fetch any entries that have been added after from_id, until to_id. This may include entries
   * with a lower or higher id that were inserted out of order. Both from_id & to_id are optional
   * (may be blank) and are treated as prefixes so that a unique prefix may be used instead of the
   * whole entry. Entries may be "deduped" just like in "get".
   * Throw "TimelineStoreException" if there's no such timeline in cache.
   */
  TimelineSegment get_range(1: string timeline_id, 2: i64 from_id, 3: i64 to_id, 4: bool dedupe) throws(1: TimelineStoreException ex)

  /*
   * Unlike the normal "get_range" call, this call will set hit=false for missing timelines instead
   * of throwing a "TimlineStoreException".
   */
  list<TimelineSegment> get_range_multi(1: list<TimelineGetRange> get_ranges)

  /*
   * Atomically set a timeline's contents.
   */
  void store(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)

  /*
   * Merge a list of timeline entries into an existing timeline. If the timeline hasn't been
   * created, silently do nothing. Merged entries will be loosely sorted according to the timeline
   * ids.
   */
  void merge(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)

  /*
   * Remove a list of timeline entries from an existing timeline. If the timeline hasn't been
   * created, silently do nothing.
   */
  void unmerge(1: string timeline_id, 2: list<binary> entries) throws(1: TimelineStoreException ex)

  /*
   * Merge entries from one timeline into another. If the destination timeline hasn't been created,
   * silently do nothing. Merged entries will be loosely sorted according to the timeline ids.
   * Return true if the source timeline was in cache, false if there was nothing to do.
   */
  bool merge_indirect(1: string dest_timeline_id, 2: string source_timeline_id) throws(1: TimelineStoreException ex)

  /*
   * Remove all the entries from one timeline out of another. If the destination timeline hasn't
   * been created, silently do nothing. Return true if the source timeline was in cache, false if
   * there was nothing to do.
   */
  bool unmerge_indirect(1: string dest_timeline_id, 2: string source_timeline_id) throws(1: TimelineStoreException ex)

  /*
   * Delete a timeline completely.
   */
  void delete_timeline(1: string timeline_id) throws(1: TimelineStoreException ex)
}
