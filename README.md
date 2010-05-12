
Haplocheirus, a home timeline storage engine.

# Goals

- Highly available, partitioned
- Structured vectors (each timeline entry is an atomic blob, not necessarily all alike)
- Homogeneous service interface
- Eliminate client side hashing
- Preserve ability to expire timelines that aren't being read
- Durable snapshots
- Idempotent/commutative

Roughly, the goals revolve around storing only as many timelines and tweets as the current memcache
solution (and allowing for growth), but reducing the expense/impact of hardware failure by having
redundant copies of the data and having snapshots on disk to speed recovery. Additionally, the
"storage format logic" will be moved to the timeline storage engine, and out of our rails stack.

Timelines can be updated out of order, and will be corrected on read. The same entry can be inserted
multiple times into the same timeline, and duplicates will be discarded.

# Non-goals

- Contain business logic for rebuilding timelines
- Transactionally durable timelines

An API will be provided for installing a pre-built timeline, but the logic for rebuilding a timeline
will remain external. Snapshots will guarantee a cold-start recovery from some recent time (probably
a few minutes ago), but incoming tweets will need to be replayed through the system to restore the
updates that have happened since the last snapshot.

# API

Timeline entries are an i64 (for ordering & duplication detection) and optional metadata.

    struct TimelineEntry {
      i64 id;
      optional binary metadata;
    }

Clients should assume the metadata is encoded in avro, though this may vary by timeline type. The
server uses only the initial i64 and treats the rest as a blob (byte array).

- `append(entry: TimelineEntry, timelines: list<string>)`

  Write an entry into the timeline names given. Appends will silently do nothing if a timeline has
  not been created using `set`.

- `delete(entry: TimelineEntry, timelines: list<string>)`

  Delete an entry from timelines. This corresponds to a deleted tweet.

- `get(timeline: string, offset: i32, length: i32): list<TimelineEntry>`

  Fetch a span of entries from a timeline. The offset & length are counted from most recent to
  oldest, so an offset of 0 is the newest entry.

- `getSince(timeline: string, entryId: i64): list<TimelineEntry>`

  Fetch any entries that have been added after entryId.
  *This may include entries with a lower id that were inserted out of order.*
  The results are always sorted by recency.

- `set(timeline: string, entries: list<TimelineEntry>)`

  Atomically set a timeline's contents.

- `merge(timeline: string, entries: list<TimelineEntry>)`

  Merge a list of timeline entries into an existing timeline. If the timeline hasn't been created,
  silently do nothing. This is meant to be used when adding a new following, for example.

- `unmerge(timeline: string, entryies: list<TimelineEntry>)`

  Remove a list of timeline entries from an existing timeline. If the timeline hasn't been created,
  silently do nothing. This is an unfollow.

# Design

Homogenous scala servers will implement the API and use gizzard for sharding and writes. This means
the timeline namespace will be split across horizontal partitions, with each partition handling its
own replication. Write operations will be handled by a pool of worker threads.

Pending some horrific new discovery, redis will be the backend storage engine. Timelines will be
"lists" in redis terminology. Periodically, `BGSAVE` will be used to create a snapshot of each redis
server's memory contents. Write operations will be pipelined to each redis server for performance
reasons. (See the performance doc.)

Deleted entries will be marked with a tombstone so that out-of-order operations always honor a
deletion as the final state. Deleted entries can't be reinstated.

## List modifications

A few new features will be added to redis to match our hopes and dreams about timeline storage. We
think these all have a good chance of being accepted by the redis maintainer.

- `RPUSHX <key> <value>`

  Like `RPUSH`, but only adds an item if the key already exists. Does not delete the contents of
  keys that have an expiration time.

- `LINSERT <key> <newvalue> <oldvalue>`

  Insert the new value before the old value in a list.

- `LDELETE <key> <value>`

  Delete a value from a list, if it exists.

- Configurable maximum size for lists

  If a maximum size is configured for lists, whenever a list is the maximum size and a new item is
  pushed to it, an item from the other end is dropped. This will always be O(1).

## Nice to have

We might like to optimize list storage in redis, because the in-memory structures have low-hanging
fruit for space savings. This is not a launch requirement, though.
