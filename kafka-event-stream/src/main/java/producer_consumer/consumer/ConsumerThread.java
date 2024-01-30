package producer_consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import producer_consumer.util.ConsoleUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singleton;

/**
 * A simple consumer thread
 * - subscribes to a topic
 * - fetches new records and prints them.
 * The thread does not stop until all records are completed or an exception is raised.
 */
public class ConsumerThread extends Thread implements ConsumerRebalanceListener {

    private final String topic;
    private final int numRecords;
    private final CountDownLatch latch;
    private volatile boolean closed;
    private int remainingRecords;
    private ConsumerHelper consumerHelper;

    public ConsumerThread(String threadName, String bootstrapServers, String topic, String groupId,
                          Optional<String> instanceId, boolean readCommitted, int numRecords, CountDownLatch latch) {
        super(threadName);
        this.topic = topic;
        this.numRecords = numRecords;
        this.remainingRecords = numRecords;
        this.latch = latch;
        this.consumerHelper = new ConsumerHelper(bootstrapServers, groupId, instanceId, readCommitted);
    }

    @Override
    public void run() {
        // the consumer instance is NOT thread safe
        try (KafkaConsumer<Integer, String> consumer = getKafkaConsumer()) {
            // subscribes to a list of topics to get dynamically assigned partitions
            // this class implements the rebalance listener that we pass here to be notified of such events
            consumer.subscribe(singleton(topic), this);
            ConsoleUtils.printOut("Subscribed to %s", topic);

            while (!closed && remainingRecords > 0) {
                try {
                    // if required, poll updates partition assignment and invokes the configured rebalance listener
                    // then tries to fetch records sequentially using the last committed offset or auto.offset.reset policy
                    // returns immediately if there are records or times out returning an empty record set
                    // the next poll must be called within session.timeout.ms to avoid group rebalance
                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<Integer, String> record : records) {
                        ConsoleUtils.maybePrintRecord(numRecords, record);
                    }
                    remainingRecords -= records.count();
                } catch (AuthorizationException | UnsupportedVersionException
                         | RecordDeserializationException e) {
                    // we can't recover from these exceptions
                    ConsoleUtils.printErr(e.getMessage());
                    shutdown();
                } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                    // invalid or no offset found without auto.reset.policy
                    ConsoleUtils.printOut("Invalid or no offset found, using latest");
                    consumer.seekToEnd(e.partitions());
                    consumer.commitSync();
                } catch (KafkaException e) {
                    // log the exception and try to continue
                    ConsoleUtils.printErr(e.getMessage());
                }
            }
        } catch (Throwable e) {
            ConsoleUtils.printOut("Unhandled exception");
        }
        ConsoleUtils.printOut("Fetched %d records", numRecords - remainingRecords);
        shutdown();
    }

    public KafkaConsumer<Integer, String> getKafkaConsumer() {
        return this.consumerHelper.createKafkaConsumer();
    }

    public void shutdown() {
        if (!closed) {
            closed = true;
            latch.countDown();
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        ConsoleUtils.printOut("Revoked partitions: %s", partitions);
        // this can be used to commit pending offsets when using manual commit and EOS is disabled
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        ConsoleUtils.printOut("Assigned partitions: %s", partitions);
        // this can be used to read the offsets from an external store or some other initialization
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        ConsoleUtils.printOut("Lost partitions: %s", partitions);
        // this is called when partitions are reassigned before we had a chance to revoke them gracefully
        // we can't commit pending offsets because these partitions are probably owned by other consumers already
        // nevertheless, we may need to do some other cleanup
    }
}
