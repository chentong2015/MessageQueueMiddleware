package producer_consumer.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import producer_consumer.util.ConsoleUtils;

public class ProducerCallback implements Callback {

    private final int key;
    private final String value;
    private final int numRecords;

    public ProducerCallback(int key, String value, int numRecords) {
        this.key = key;
        this.value = value;
        this.numRecords = numRecords;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
     * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
     *                 with -1 value for all fields except for topicPartition will be returned if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            ConsoleUtils.printErr(exception.getMessage());
            if (!(exception instanceof RetriableException)) {
                // we can't recover from these exceptions
                // shutdown();
            }
        } else {
            ConsoleUtils.maybePrintRecord(numRecords, key, value, metadata);
        }
    }
}