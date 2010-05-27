#!/bin/sh
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

if java -version 2>&1 |grep "1\.5"; then
  echo "Java must be at least 1.6"
  exit 1
fi

#if [ "x$DB_USERNAME" = "x" -o "x$DB_PASSWORD" = "x" ]; then
#  echo "Please set DB_USERNAME and DB_PASSWORD."
#  exit 1
#fi

echo "Killing any running haplo..."
curl http://localhost:7667/shutdown >/dev/null 2>/dev/null
sleep 3

echo "Launching haplo..."
#echo "DROP DATABASE IF EXISTS timelines_development" | mysql -u$DB_USERNAME -p$DB_PASSWORD
#echo "CREATE DATABASE IF NOT EXISTS timelines_development" | mysql -u$DB_USERNAME -p$DB_PASSWORD

JAVA_OPTS="-Xms256m -Xmx256m -XX:NewSize=64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -server"
java -Dstage=development $JAVA_OPTS -jar ./dist/haplocheirus/haplocheirus-1.0.jar &
sleep 5
./src/scripts/haplo.rb setup-dev
