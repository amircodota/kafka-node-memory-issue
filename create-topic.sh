#!/usr/bin/env bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 100 --replication-factor 1 --topic mem-issue-test