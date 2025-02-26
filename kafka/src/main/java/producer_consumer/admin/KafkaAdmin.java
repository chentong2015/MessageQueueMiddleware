package producer_consumer.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import producer_consumer.util.ConsoleUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// 使用Kafka Admin修改并重新创建Topics
public class KafkaAdmin {

    // Delete all topics if exist and then create NewTopic
    public static void recreateTopics(String bootstrapServers, int numPartitions, String... topicNames) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        try (Admin admin = Admin.create(props)) {
            try {
                admin.deleteTopics(Arrays.asList(topicNames)).all().get();
            } catch (ExecutionException e) {
                ConsoleUtils.printErr("Topics deletion error: %s", e.getCause());
            }
            ConsoleUtils.printOut("Deleted topics: %s", Arrays.toString(topicNames));
            createTopics(admin, numPartitions, topicNames);
        } catch (Throwable e) {
            throw new RuntimeException("Topics creation error", e);
        }
    }

    // Create topics in a retry loop
    private static void createTopics(Admin admin, int numPartitions, String... topicNames) throws Exception {
        while (true) {
            // use default RF to avoid NOT_ENOUGH_REPLICAS error with minISR > 1
            short replicationFactor = -1;
            List<NewTopic> newTopics = Arrays.stream(topicNames)
                    .map(name -> new NewTopic(name, numPartitions, replicationFactor))
                    .collect(Collectors.toList());
            try {
                admin.createTopics(newTopics).all().get();
                ConsoleUtils.printOut("Created topics: %s", Arrays.toString(topicNames));
                break;
            } catch (ExecutionException e) {
                ConsoleUtils.printOut("Waiting for topics metadata cleanup");
                TimeUnit.MILLISECONDS.sleep(1_000);
            }
        }
    }
}
