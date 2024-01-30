package producer_consumer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import producer_consumer.util.ConsoleUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * A simple producer thread supporting two send modes:
 * - Async mode (default): records are sent without waiting for the response.
 * - Sync mode: each send operation blocks waiting for the response.
 */
public class ProducerThread extends Thread {

    private final String topic;
    private final boolean isAsync;
    private final int numRecords;
    private final CountDownLatch latch;
    private volatile boolean closed;
    private ProducerHelper producerHelper;

    public ProducerThread(String threadName, String bootstrapServers, String topic, boolean isAsync,
                          String transactionalId, boolean enableIdempotency, int numRecords,
                          int transactionTimeoutMs, CountDownLatch latch) {
        super(threadName);
        this.topic = topic;
        this.isAsync = isAsync;
        this.numRecords = numRecords;
        this.latch = latch;
        this.producerHelper = new ProducerHelper(bootstrapServers, transactionalId, enableIdempotency, transactionTimeoutMs);
    }

    // The producer instance is thread safe
    // 生产者发送完指定数量的record之后便会停止
    @Override
    public void run() {
        int sentRecords = 0;

        try (KafkaProducer<Integer, String> producer = getKafkaProducer()) {
            while (!closed && sentRecords < numRecords) {
                if (isAsync) {
                    asyncSend(producer, sentRecords, "test value" + sentRecords);
                } else {
                    syncSend(producer, sentRecords, "test value" + sentRecords);
                }
                sentRecords++;
            }
        } catch (Throwable e) {
            ConsoleUtils.printOut("Unhandled exception");
        }
        ConsoleUtils.printOut("Sent %d records", sentRecords);
        shutdown();
    }

    public KafkaProducer<Integer, String> getKafkaProducer() {
        return this.producerHelper.createKafkaProducer();
    }

    // 带有Callback回调方法的异步发送
    private void asyncSend(KafkaProducer<Integer, String> producer, int key, String value) {
        // send the record asynchronously, setting a callback to be notified of the result
        // note that, even if you set a small batch.size with linger.ms=0, the send operation
        // will still be blocked when buffer.memory is full or metadata are not available
        producer.send(new ProducerRecord<>(topic, key, value), new ProducerCallback(key, value, numRecords));
    }

    // 同步发送消息Record(其中包含Topic主题和发送的具体内容), 返回发送结果元信息RecordMetadata
    private RecordMetadata syncSend(KafkaProducer<Integer, String> producer, int key, String value)
        throws ExecutionException, InterruptedException {
        try {
            // send the record and then call get, which blocks waiting for the ack from the broker
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
            ConsoleUtils.maybePrintRecord(numRecords, key, value, metadata);
            return metadata;
        } catch (AuthorizationException | UnsupportedVersionException | ProducerFencedException
                 | FencedInstanceIdException | OutOfOrderSequenceException | SerializationException e) {
            ConsoleUtils.printErr(e.getMessage());
            // we can't recover from these exceptions
            shutdown();
        } catch (KafkaException e) {
            ConsoleUtils.printErr(e.getMessage());
        }
        return null;
    }

    public void shutdown() {
        if (!closed) {
            closed = true;
            latch.countDown();
        }
    }
}
