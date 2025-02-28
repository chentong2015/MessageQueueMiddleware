#!/bin/bash

base_dir=$(dirname $0)/../..

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx512M"
fi

# 使用kafka-run-class脚本来运行指定的main class
# main class的运行带有执行的参数
exec $base_dir/bin/kafka-run-class.sh producer_consumer.DemoKafkaConsumerProducer $@