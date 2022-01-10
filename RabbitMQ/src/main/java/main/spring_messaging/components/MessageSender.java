package main.spring_messaging.components;

import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class MessageSender {

    // Test: 做到界面输入框进行发送
    public static void main(String[] args) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate();
        String message = "Hello from Spring RabbitMQ";
        rabbitTemplate.convertAndSend("topic-exchange", "com.tong.demo", message);
        System.out.println("Send message: Success !");
    }
}
