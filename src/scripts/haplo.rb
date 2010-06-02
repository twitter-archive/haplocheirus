#!/usr/bin/env ruby

$:.push(File.dirname($0))
require 'optparse'
require 'socket'
require 'simple'

ShardInfo = ThriftClient::Simple.make_struct(:ShardInfo,
  ThriftClient::Simple::Field.new(:class_name, ThriftClient::Simple::STRING, 1),
  ThriftClient::Simple::Field.new(:table_prefix, ThriftClient::Simple::STRING, 2),
  ThriftClient::Simple::Field.new(:hostname, ThriftClient::Simple::STRING, 3),
  ThriftClient::Simple::Field.new(:source_type, ThriftClient::Simple::STRING, 4),
  ThriftClient::Simple::Field.new(:destination_type, ThriftClient::Simple::STRING, 5),
  ThriftClient::Simple::Field.new(:busy, ThriftClient::Simple::I32, 6),
  ThriftClient::Simple::Field.new(:shard_id, ThriftClient::Simple::I32, 7))

ShardChild = ThriftClient::Simple.make_struct(:ShardChild,
  ThriftClient::Simple::Field.new(:shard_id, ThriftClient::Simple::I32, 1),
  ThriftClient::Simple::Field.new(:weight, ThriftClient::Simple::I32, 2))

Forwarding = ThriftClient::Simple.make_struct(:Forwarding,
  ThriftClient::Simple::Field.new(:table_id, ThriftClient::Simple::I32, 1),
  ThriftClient::Simple::Field.new(:base_id, ThriftClient::Simple::I64, 2),
  ThriftClient::Simple::Field.new(:shard_id, ThriftClient::Simple::I32, 3))

ShardMigration = ThriftClient::Simple.make_struct(:ShardMigration,
  ThriftClient::Simple::Field.new(:source_shard_id, ThriftClient::Simple::I32, 1),
  ThriftClient::Simple::Field.new(:destination_shard_id, ThriftClient::Simple::I32, 2),
  ThriftClient::Simple::Field.new(:replicating_shard_id, ThriftClient::Simple::I32, 3),
  ThriftClient::Simple::Field.new(:write_only_shard_id, ThriftClient::Simple::I32, 4))

class ShardManager < ThriftClient::Simple::ThriftService
  thrift_method :create_shard, i32, field(:shard, struct(ShardInfo), 1)
  thrift_method :find_shard, i32, field(:shard, struct(ShardInfo), 1)
  thrift_method :get_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :update_shard, void, field(:shard, struct(ShardInfo), 1)
  thrift_method :delete_shard, void, field(:shard_id, i32, 1)

  thrift_method :add_child_shard, void, field(:parent_shard_id, i32, 1), field(:child_shard_id, i32, 2), field(:weight, i32, 3)
  thrift_method :remove_child_shard, void, field(:parent_shard_id, i32, 1), field(:child_shard_id, i32, 2)
  thrift_method :replace_child_shard, void, field(:old_child_shard_id, i32, 1), field(:new_child_shard_id, i32, 2)
  thrift_method :list_shard_children, list(struct(ShardChild)), field(:shard_id, i32, 1)

  thrift_method :mark_shard_busy, void, field(:shard_id, i32, 1), field(:busy, i32, 2)
  thrift_method :copy_shard, void, field(:source_shard_id, i32, 1), field(:destination_shard_id, i32, 2)
  thrift_method :setup_migration, struct(ShardMigration), field(:source_shard_info, struct(ShardInfo), 1), field(:destination_shard_info, struct(ShardInfo), 2)
  thrift_method :migrate_shard, void, field(:migration, struct(ShardMigration), 1)

  thrift_method :set_forwarding, void, field(:forwarding, struct(Forwarding), 1)
  thrift_method :replace_forwarding, void, field(:old_shard_id, i32, 1), field(:new_shard_id, i32, 2)
  thrift_method :get_forwarding, struct(ShardInfo), field(:table_id, i32, 1), field(:base_id, i64, 2)
  thrift_method :get_forwarding_for_shard, struct(Forwarding), field(:shard_id, i32, 1)
  thrift_method :get_forwardings, list(struct(Forwarding))
  thrift_method :reload_forwardings, void
  thrift_method :find_current_forwarding, struct(ShardInfo), field(:table_id, i32, 1), field(:id, i64, 2)

  thrift_method :shard_ids_for_hostname, list(i32), field(:hostname, string, 1), field(:class_name, string, 2)
  thrift_method :shards_for_hostname, list(struct(ShardInfo)), field(:hostname, string, 1), field(:class_name, string, 2)
  thrift_method :get_busy_shards, list(struct(ShardInfo))
  thrift_method :get_parent_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :get_root_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :get_child_shards_of_class, list(struct(ShardInfo)), field(:parent_shard_id, i32, 1), field(:class_name, string, 2)

  thrift_method :rebuild_schema, void
end

class JobManager < ThriftClient::Simple::ThriftService
  thrift_method :retry_errors, void
  thrift_method :stop_writes, void
  thrift_method :resume_writes, void
  thrift_method :retry_errors_for, void, field(:priority, i32, 1)
  thrift_method :stop_writes_for, void, field(:priority, i32, 1)
  thrift_method :resume_writes_for, void, field(:priority, i32, 1)
  thrift_method :is_writing, bool, field(:priority, i32, 1)
  thrift_method :inject_job, void, field(:priority, i32, 1), field(:job, string, 2)
end

class TimelineStore < ThriftClient::Simple::ThriftService
  thrift_method :append, void, field(:entry, string, 1), field(:timeline_ids, list(string), 2)
  thrift_method :remove, void, field(:entry, string, 1), field(:timeline_ids, list(string), 2)
  thrift_method :get, list(string), field(:timeline, string, 1), field(:offset, i32, 2), field(:length, i32, 3)
end

SHARD_PORT = 7668
JOB_PORT = 7669
TIMELINE_PORT = 7666

# ruby 1.9 is incompatible with 1.8 :(
def ord(char)
  char.ord
rescue
  char[0]
end

def connect_shard_service(hostname)
  ShardManager.new(TCPSocket.new(hostname, SHARD_PORT))
end

def connect_job_service(hostname)
  JobManager.new(TCPSocket.new(hostname, JOB_PORT))
end

def connect_timeline_service(hostname)
  TimelineStore.new(TCPSocket.new(hostname, TIMELINE_PORT))
end

def command_setup_dev
  service = connect_shard_service("localhost")
  shard = service.create_shard(ShardInfo.new("com.twitter.haplocheirus.RedisShard", "dev1", "localhost", "", "", 0))
  service.set_forwarding(Forwarding.new(0, 0, shard))
  service.reload_forwardings()
end

def command_append(timeline_name, entry)
  timeline = connect_timeline_service("localhost")
  timeline.append(entry, [ timeline_name ])
end

def command_remove(timeline_name, entry)
  timeline = connect_timeline_service("localhost")
  timeline.remove(entry, [ timeline_name ])
end

def command_get(timeline_name, offset, length)
  timeline = connect_timeline_service("localhost")
  items = timeline.get(timeline_name, offset, length)
  items.each { |i| puts i.inspect }
end

def command_spam(pushes, timeline_count)
  timeline = TimelineStore.new(TCPSocket.new("localhost", TIMELINE_PORT), 100)
  entry = ("\x00" * 15) + "\x01"
  current_timeline = 0

  start_time = Time.now
  i = 0
  while i < pushes
    names = (0..9).map { |n| "timeline:#{current_timeline + n}" }
    timeline.append(entry, names)
    current_timeline = (current_timeline + 1) % timeline_count
    i += 10
  end
  end_time = Time.now

  puts "Pushed #{pushes} items in #{((end_time.to_f - start_time.to_f) * 1000).to_i} msec."
end

case ARGV[0]
when "setup-dev"
  command_setup_dev
when "append"
  command_append(ARGV[1], ARGV[2])
when "remove"
  command_remove(ARGV[1], ARGV[2])
when "get"
  command_get(ARGV[1], ARGV[2].to_i, ARGV[3].to_i)
when "spam"
  command_spam(ARGV[1].to_i, 100)
when "flush"
  connect_job_service("localhost").retry_errors
end
