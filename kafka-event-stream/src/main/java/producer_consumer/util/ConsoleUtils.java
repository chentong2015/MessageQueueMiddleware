package producer_consumer.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import static java.lang.String.format;

// Console端输出格式的工具类
public class ConsoleUtils {

    private ConsoleUtils() {
    }

    public static void printHelp(String message, Object... args) {
        System.out.println(format(message, args));
    }

    public static void printOut(String message, Object... args) {
        System.out.printf("%s - %s%n", Thread.currentThread().getName(), format(message, args));
    }

    public static void printErr(String message, Object... args) {
        System.err.printf("%s - %s%n", Thread.currentThread().getName(), format(message, args));
    }

    public static void maybePrintRecord(long numRecords, ConsumerRecord<Integer, String> record) {
        maybePrintRecord(numRecords, record.key(), record.value(), record.topic(), record.partition(), record.offset());
    }

    public static void maybePrintRecord(long numRecords, int key, String value, RecordMetadata metadata) {
        maybePrintRecord(numRecords, key, value, metadata.topic(), metadata.partition(), metadata.offset());
    }

    private static void maybePrintRecord(long numRecords, int key, String value, String topic, int partition, long offset) {
        // we only print 10 records when there are 20 or more to send
        if (key % Math.max(1, numRecords / 10) == 0) {
            printOut("Sample: record(%d, %s), partition(%s-%d), offset(%d)", key, value, topic, partition, offset);
        }
    }
}
