package producer_consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public class ConsumerHelper {

    private final String bootstrapServers;
    private final String groupId;
    private final Optional<String> instanceId;
    private final boolean readCommitted;

    public ConsumerHelper(String bootstrapServers, String groupId, Optional<String> instanceId, boolean readCommitted) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.instanceId = instanceId;
        this.readCommitted = readCommitted;
    }

    public KafkaConsumer<Integer, String> createKafkaConsumer() {
        Properties props = new Properties();
        // bootstrap server config is required for consumer to connect to brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // client id is not required, but it's good to track the source of requests beyond just ip/port
        // by allowing a logical application name to be included in server-side request logging
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        // consumer group id is required when we use subscribe(topics) for group management
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // sets static membership to improve availability (e.g. rolling restart)
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        // disables auto commit when EOS is enabled, because offsets are committed with the transaction
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, readCommitted ? "false" : "true");
        // key and value are just byte arrays, so we need to set appropriate deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        if (readCommitted) {
            // skips ongoing and aborted transactions
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        // sets the reset offset policy in case of invalid or no offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }
}
