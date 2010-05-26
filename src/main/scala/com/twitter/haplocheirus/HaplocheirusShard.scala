package com.twitter.haplocheirus

import com.twitter.gizzard.shards._


trait HaplocheirusShard extends Shard {
  @throws(classOf[ShardException]) def append(entry: Array[Byte], timeline: String)
  @throws(classOf[ShardException]) def remove(entry: Array[Byte], timeline: String)
  @throws(classOf[ShardException]) def get(timeline: String, offset: Int, length: Int): Seq[Array[Byte]]
}

class HaplocheirusShardAdapter(shard: ReadWriteShard[HaplocheirusShard])
      extends ReadWriteShardAdapter(shard) with HaplocheirusShard {
  def append(entry: Array[Byte], timeline: String) = shard.writeOperation(_.append(entry, timeline))
  def remove(entry: Array[Byte], timeline: String) = shard.writeOperation(_.remove(entry, timeline))
  def get(timeline: String, offset: Int, length: Int) = shard.readOperation(_.get(timeline, offset, length))
}
