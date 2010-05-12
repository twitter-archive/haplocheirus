namespace java com.twitter.haplocheirus.thrift
namespace ruby Haplocheirus


service TimelineStore {
  void append(binary entry, list<string> timeline_ids)
}
