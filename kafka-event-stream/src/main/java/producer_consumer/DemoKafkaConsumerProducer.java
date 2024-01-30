package producer_consumer;

import producer_consumer.admin.KafkaAdmin;
import producer_consumer.consumer.ConsumerThread;
import producer_consumer.producer.ProducerThread;
import producer_consumer.util.KafkaProperties;
import producer_consumer.util.ConsoleUtils;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This example can be decomposed into the following stages:
 * <p>
 * 1. Clean any topics left from previous runs.
 * 2. Create a producer thread to send a set of records to topic1.
 * 3. Create a consumer thread to fetch all previously sent records from topic1.
 * <p>
 * If you are using IntelliJ IDEA, the above arguments should be put in `Modify Run Configuration - Program Arguments`.
 * You can also set an output log file in `Modify Run Configuration - Modify options - Save console output to file` to
 * record all the log output together.
 */
public class DemoKafkaConsumerProducer {

    public static final String TOPIC_NAME = "my-topic-1";
    public static final String GROUP_NAME = "my-group";

    // TODO. 创建的Consumer和Producer都是异步线程，不会阻塞主线程
    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                ConsoleUtils.printHelp("This example takes 2 parameters (i.e. 10000 sync):%n" +
                    "- records: total number of records to send (required)%n" +
                    "- mode: pass 'sync' to send records synchronously (optional)");
                return;
            }

            int numRecords = Integer.parseInt(args[0]);
            boolean isAsync = args.length == 1 || !args[1].trim().equalsIgnoreCase("sync");

            KafkaAdmin.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, -1, TOPIC_NAME);
            CountDownLatch latch = new CountDownLatch(2);

            ProducerThread producerThread = new ProducerThread("producer", KafkaProperties.BOOTSTRAP_SERVERS,
                    TOPIC_NAME, isAsync, null, false, numRecords, -1, latch);
            producerThread.start();
            ConsumerThread consumerThread = new ConsumerThread("consumer", KafkaProperties.BOOTSTRAP_SERVERS,
                    TOPIC_NAME, GROUP_NAME, Optional.empty(), false, numRecords, latch);
            consumerThread.start();

            if (!latch.await(5, TimeUnit.MINUTES)) {
                ConsoleUtils.printErr("Timeout after 5 minutes waiting for termination");
                producerThread.shutdown();
                consumerThread.shutdown();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
