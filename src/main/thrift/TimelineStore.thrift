namespace java com.twitter.haplocheirus.thrift
namespace ruby Haplocheirus


service TimelineStore {
  void append(1: binary entry, 2: list<string> timeline_ids);
  void remove(1: binary entry, 2: list<string> timeline_ids);
  list<binary> filter(1: string timeline_id, 2: list<binary> entry);
  list<binary> get(1: string timeline_id, 2: i32 offset, 3: i32 length, 4: bool dedupe);
  list<binary> get_since(1: string timeline_id, 2: i64 from_id, 3: bool dedupe);
  void store(1: string timeline_id, 2: list<binary> entries);
  void merge(1: string timeline_id, 2: list<binary> entries);
  void unmerge(1: string timeline_id, 2: list<binary> entries);
  void delete_timeline(1: string timeline_id);
}
