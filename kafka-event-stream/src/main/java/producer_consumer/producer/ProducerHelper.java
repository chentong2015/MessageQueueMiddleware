package producer_consumer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class ProducerHelper {

    private final String bootstrapServers;
    private final String transactionalId;
    private final boolean enableIdempotency;
    private final int transactionTimeoutMs;

    public ProducerHelper(String bootstrapServers, String transactionalId, boolean enableIdempotency, int transactionTimeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.transactionalId = transactionalId;
        this.enableIdempotency = enableIdempotency;
        this.transactionTimeoutMs = transactionTimeoutMs;
    }

    public KafkaProducer<Integer, String> createKafkaProducer() {
        Properties props = new Properties();
        // bootstrap server config is required for producer to connect to brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // client id is not required, but it's good to track the source of requests beyond just ip/port
        // by allowing a logical application name to be included in server-side request logging
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        // key and value are just byte arrays, so we need to set appropriate serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (transactionTimeoutMs > 0) {
            // max time before the transaction coordinator proactively aborts the ongoing transaction
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        }
        if (transactionalId != null) {
            // the transactional id must be static and unique
            // it is used to identify the same producer instance across process restarts
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        // enable duplicates protection at the partition level
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);
        return new KafkaProducer<>(props);
    }
}
