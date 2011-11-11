#!/usr/bin/env ruby
#
# Copyright 2010 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

$:.push(File.dirname($0))
require 'optparse'
require 'yaml'

$options = {
  :config_filename => ENV['HOME'] + "/.shards.yml",
  :count => 500,
  :constraints => true,
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [options]"
  opts.separator "Example: #{$0} -f shards.yml"

  opts.on("-f", "--config=FILENAME", "load shard database config (default: #{$options[:config_filename]})") do |filename|
    $options[:config_filename] = filename
  end
  opts.on("-n", "--count=N", "create N bins (default: #{$options[:count]})") do |count|
    $options[:count] = count.to_i
  end
  opts.on("-c", "--disable-constraints", "Disables the rack diversity constraints") do
    $options[:constraints] = false
  end
end

parser.parse!(ARGV)

config = begin
  YAML.load_file($options[:config_filename])
rescue => e
  puts "Exception while reading config file: #{e}"
  {}
end

app_host, app_port = (config['app_host'] || 'localhost').split(':')
app_port ||= 7668

namespace = config['namespace'] || nil
db_trees = Array(config['databases'] || 'localhost')

gizzmo = lambda do |cmd|
  `gizzmo --host=#{app_host} --port=#{app_port} #{cmd}`
end

# This isn't strictly random. It includes a constraint check to prevent both
# replicas in the set being either on the same host as each other, on the same
# pair of hosts as another replica set or part of a hotspot (too many pairs/rack).
# It also isn't guaranteed to converge and doesn't backtrack when it hits an impossible
# state.
#
# Caveat executor.
#
class RandomDistribution

  MAX_PER_RACK = 2

  # Array of hostnames and the number of shards per host.
  # Optionally, and initial shard number. Defaults to 0.
  def initialize(hosts, shard_count, initial_shard = 0)
    raise(ArgumentError, "no hosts") if hosts.empty?
    @hosts = hosts
    @shards = @hosts.inject({}) do |acc, h|
      acc[h] = (initial_shard...(initial_shard + shard_count)).map { |i| "#{h}:#{i}" }
      acc
    end

    if hosts.length * shard_count % 2 != 0
      @shards[hosts[0]].delete_at(-1)
    end

    # poorly named...
    @forward  = {} # shard => shard
    @backward = {} # host  => host
    @racks    = {} # rack  => pair_count

    shuffle
  end

  def size
    @forward.keys.size
  end
  
  def each(&block) #:yields: host:port => host:port
    @forward.each(&block)
  end

  def shuffle
    until shards_empty?
      @hosts.each do |host|
        if @shards[host].any?
          shard = @shards[host].pop
          pair = nil

          if $options[:constraints]
            until pair && check_pair(host, pair)
              pair = random_pair(host)
            end
          else
            pair = random_pair(host)
          end

          pair_shard = @shards[pair].pop

          @forward[shard] = pair_shard
          @backward[pair] ||= []
          @backward[pair] << host
        end
      end
    end
  end

  def random_pair(host)
    p = host
    p = @hosts[rand(@hosts.size)] until (!$options[:contraints] || p != host) && @shards[p].any?
    p
  end

  def rack_of(hostname)
    hostname.split(/-/)[2]
  end

  def cas_rack(a, b)
    a_rack = rack_of(a)
    b_rack = rack_of(b)

    if a_rack == b_rack
      @racks[a_rack] ||= 0 # Number of pairs in this rack
      return false if @racks[a_rack] > MAX_PER_RACK
      @racks[a_rack] += 1
    end
    true
  end

  def check_pair(a, b)
    return false if @backward[a] && @backward[a].include?(b)
    return false if @backward[b] && @backward[b].include?(a)
    return cas_rack(a, b)
  end

  def shards_empty?
    @shards.each do |_, a|
      if a.any?
        @hosts.shuffle!
        return false
      end
    end
    true
  end
end

distribution = RandomDistribution.new(db_trees, 7, 6379)
shards_per_forwarding = (($options[:count] - 1) / distribution.size) + 1

puts "Creating bins"
STDOUT.flush

current_shard = 0
weight = 4 # FIXME: fixed weight


puts $options[:count]
shards_per_forwarding.times do |i|
  # FIXME: fixed replication factor of 2
  distribution.each do |pair_1, pair_2|
    if current_shard >= $options[:count]
      break
    end
    table_name = [ namespace, "haplo_%04d" % current_shard ].compact.join("_")
    gizzmo.call "create com.twitter.gizzard.shards.ReplicatingShard localhost/#{table_name}_replicating"

    gizzmo.call "create com.twitter.haplocheirus.RedisShard #{pair_1}/#{table_name}_1 #{pair_2}/#{table_name}_2"
    gizzmo.call "addlink localhost/#{table_name}_replicating #{pair_1}/#{table_name}_1 #{weight}"
    gizzmo.call "addlink localhost/#{table_name}_replicating #{pair_2}/#{table_name}_2 #{weight}"

    lower_bound = (1 << 60) / $options[:count] * current_shard
    gizzmo.call "addforwarding -- 0 #{lower_bound} localhost/#{table_name}_replicating"

    current_shard += 1
    $stdout.flush
  end
end

puts "Done."
