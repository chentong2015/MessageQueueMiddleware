package main.exchange_pattern;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MessageSender {

    private final static String QUEUE_NAME = "DEMO1";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        // connect to a RabbitMQ node on the local machine
        factory.setHost("localhost");
        // 使用try-with-resources来关闭Connection和Channel
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明要发送到的消息队列
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            // 在消息队列中发布指定的消息: push messages asynchronously 异步发送消息
            // 不需要等待Receiver端的回复
            String message = "First Message";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println("Sent '" + message + "'");

            Thread.sleep(3000);
            message = "Exit";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        }
    }
}
