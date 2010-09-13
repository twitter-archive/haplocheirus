namespace java com.twitter.haplocheirus.thrift
namespace ruby Haplocheirus


exception TimelineStoreException {
  1: string description
}

struct TimelineSegment {
  /*
   * Timeline entries are opaque binary data. Two assumptions are made about the contents of each
   * entry:
   *
   *   1. The first i64 (little-endian) is a sort key. Two entries with the same sort key will be
   *      considered equivalent (without looking at the rest of the entry) and only one will be
   *      returned.
   *   2. The second i64 (little-endian) is an optional deduping key if "dedupe=true" is passed
   *      to get or get_range. It's treated just like the sort key, and if two entries have the
   *      same dedupe key, only one is returned.
   *
   * Other than that, haplocheirus will make no assumptions about the content or size of each
   * timeline entry. Entries can vary by size within the same timeline, and may be as small as a
   * single i64.
   */
  1: list<binary> entries;

  /*
   * The actual full length of the timeline as stored in redis.
   */
  2: i32 size;
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
   */
  list<binary> filter(1: string timeline_id, 2: list<binary> entry, 3: i32 max_search) throws(1: TimelineStoreException ex)

  /*
   * Fetch a span of entries from a timeline. The offset & length are counted from most recent to
   * oldest, so an offset of 0 is the newest entry. If "dedupe" is true, only entries with unique
   * dedupe keys (the 2nd i64 of a timeline entry) will be returned.
   */
  TimelineSegment get(1: string timeline_id, 2: i32 offset, 3: i32 length, 4: bool dedupe) throws(1: TimelineStoreException ex)

  /*
   * Fetch any entries that have been added after from_id, until to_id. This may include entries
   * with a lower or higher id that were inserted out of order. Both from_id & to_id are optional
   * (may be blank) and are treated as prefixes so that a unique prefix may be used instead of the
   * whole entry. Entries may be "deduped" just like in "get".
   */
  TimelineSegment get_range(1: string timeline_id, 2: i64 from_id, 3: i64 to_id, 4: bool dedupe) throws(1: TimelineStoreException ex)

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
   * Delete a timeline completely.
   */
  void delete_timeline(1: string timeline_id) throws(1: TimelineStoreException ex)
}
