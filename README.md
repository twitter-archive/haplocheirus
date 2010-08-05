
# Haplocheirus

Haplocheirus is a redis-backed storage engine for timelines.

**Disclaimer**: This project is an experiment, and is not complete or deployed anywhere yet.

Timelines are lists of 64-bit ids, possibly with a small bit of attached metadata, in preserved
order. New entries are always added at the "front". Old entries may be dropped off the "back" if the
timeline exceeds a maximum size. Two timelines can be merged by assuming they're roughly sorted by
id. A timeline query returns a slice of the timeline, newest first.

## Goals

- Highly available, partitioned
- Structured vectors (each timeline entry is an atomic blob, not necessarily all alike)
- Homogeneous service interface
- Eliminate client side hashing
- Preserve ability to expire timelines that aren't being read
- Durable snapshots
- Idempotent/commutative

## Non-goals

- Contain business logic for building timelines from scratch
- Transactionally durable timelines

## Structure

[Gizzard](http://github.com/twitter/gizzard) is used to handle partitioning and job queueing, and
[Redis](http://code.google.com/p/redis/) is used as the backend storage for each shard.

Redis has been patched to have a few extra features. Currently this lives as a branch on github
here: <http://github.com/robey/redis/commits/twitter2>. Eventually we hope to have these patches
merged back into redis in some form.

## Building

You need:
- java 1.6
- thrift 0.2.0
- sbt 0.7.4
- redis-server (twitter2 branch; see above)

You might want:
- haplocheirus-client <http://github.com/bitbckt/haplocheirus-client>

A special build of jredis is used, too, but currently the jar is included in the repo.

Then:

    $ sbt clean update package-dist

## Running

Start up your local redis server, then:

    $ ./dist/haplocheirus-1.0/scripts/setup-env.sh

## Community

License: Apache 2 (see included LICENSE file)

- IRC: #twinfra on freenode (irc.freenode.net)
