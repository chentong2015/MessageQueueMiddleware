package main.spring_messaging.config;

import main.spring_messaging.components.MessageReceiver;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebMvc
public class SpringMessagingConfig implements WebMvcConfigurer {

    // 通过系统Application配置文件动态设置信息
    @Value("${spring.rabbitmq.name}")
    private String queueName;

    @Value("${spring.rabbitmq.exchange}")
    private String topicName;

    @Value("${spring.rabbitmq.routingkey}")
    private String routingKey;

    @Bean
    Queue queue() {
        return new Queue(queueName, false);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange(topicName);
    }

    // 绑定创建的指定队列和消息的主题，key键值确保发送和接收消息的准确性
    // Any messages sent with a routing key that begins with "com.tong" are routed to the queue
    @Bean
    Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(routingKey);
    }

    // TODO: 使用的是默认的配置信息
    //  MessageListenerContainer and Receiver beans are all you need to listen for messages
    @Bean
    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueName);
        container.setMessageListener(listenerAdapter);
        return container;
    }

    // 消息的监听器，监听指定队列名称中的消息，并调用指定的方法来处理
    // This method is registered as a message listener in the container
    // It listens for messages on the spring-boot queue
    // It invokes receiveMessage function
    @Bean
    MessageListenerAdapter listenerAdapter(MessageReceiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }
}
