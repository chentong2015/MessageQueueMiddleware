package main.spring_messaging.components;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.CommandLineRunner;

import java.util.concurrent.TimeUnit;

// @Component
public class Runner implements CommandLineRunner {

    // Runner bean, which is then automatically run
    // It retrieves the RabbitTemplate from the application context
    // Sends a message on the spring-boot queue

    private final RabbitTemplate rabbitTemplate;
    private final Receiver receiver;

    public Runner(Receiver receiver, RabbitTemplate rabbitTemplate) {
        this.receiver = receiver;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Sending message...");
        String message = "Hello from RabbitMQ!";
        String routingKey = "com.tong.demo";
        rabbitTemplate.convertAndSend("topic-exchange", routingKey, message);
        receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
    }
}
