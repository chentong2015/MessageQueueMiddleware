#!/bin/bash

base_dir=$(dirname $0)/../..

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx512M"
fi

exec $base_dir/bin/kafka-run-class.sh kafka.examples.KafkaExactlyOnceDemo $@
