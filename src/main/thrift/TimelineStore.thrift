namespace java com.twitter.haplocheirus.thrift
namespace ruby Haplocheirus


service TimelineStore {
  void append(1: binary entry, 2: list<string> timeline_ids);
  void remove(1: binary entry, 2: list<string> timeline_ids);
  list<binary> get(1: string timeline_id, 2: i32 offset, 3: i32 length);
  list<binary> get_range(1: string timeline_id, 2: i64 from_id, 3: i64 to_id);
  void store(1: string timeline_id, 2: list<binary> entries);
  void merge(1: string timeline_id, 2: list<binary> entries);
  void unmerge(1: string timeline_id, 2: list<binary> entries);
}
