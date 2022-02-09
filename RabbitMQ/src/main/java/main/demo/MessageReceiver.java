package main.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class MessageReceiver {

    private final static String QUEUE_NAME = "DEMO";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        // 使用默认的连接端口
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明要接收信息的消息队列，确保在消费信息之前队列存在
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("[*] Waiting for messages. To exit press CTRL+C");

        // 由于是异步发送消息，当可以使用消息的时候，使用"回调方法"来buffer the messages
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("[x] Received '" + message + "'");
            if (message.equals("Exit")) {
                System.out.println("Connection close !");
                connection.close();
            }
        };
        // Keep the consumer running to listen for messages
        // 持续监听消息队列中可以消费的消息，使用回调方法进行处理
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });

    }
}
