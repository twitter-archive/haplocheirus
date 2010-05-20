package com.twitter.haplocheirus

import com.twitter.gizzard.shards._


trait HaplocheirusShard extends Shard {
  @throws(classOf[ShardException]) def append(entry: Array[Byte], timeline: String)
}

class HaplocheirusShardAdapter(shard: ReadWriteShard[HaplocheirusShard])
      extends ReadWriteShardAdapter(shard) with HaplocheirusShard {
  def append(entry: Array[Byte], timeline: String) = shard.writeOperation(_.append(entry, timeline))
}
