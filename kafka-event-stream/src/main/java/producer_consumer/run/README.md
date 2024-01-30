# Kafka client examples
This module contains some Kafka client examples.
Start a Kafka 2.5+ local cluster with a plain listener configured on port 9092. 

Run `examples/bin/java-producer-consumer-demo.sh 10000` 
 - asynchronously send 10k records to topic1 and consume them.
Run `examples/bin/java-producer-consumer-demo.sh 10000 sync` 
 - synchronous send 10k records to topic1 and consume them. 

Run `examples/bin/exactly-once-demo.sh 6 3 10000`  
 - create input-topic and output-topic with 6 partitions each
 - start 3 transactional application instances and process 10k records.
