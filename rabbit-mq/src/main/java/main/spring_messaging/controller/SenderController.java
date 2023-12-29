package main.spring_messaging.controller;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SenderController {

    // 字段注入，构造器注入，setter注入
    // 这里的RabbitTemplate会从Application Context中获取
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Value("${spring.rabbitmq.exchange}")
    private String topicName;

    @ResponseBody
    @GetMapping("/index")
    public String index() {
        String message = "Hello from Spring RabbitMQ";
        rabbitTemplate.convertAndSend(topicName, "com.tong.demo", message);
        return "Send message for the topic: " + topicName;
    }
}
