package apache.activemq.main;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.*;

// TODO. ConnectionFactory的创建需要提供brokerURL，同时启动
public class ApacheActiveMq {

    // 需要消息转发的Server提供消息发送的服务
    private static final String URL = "tcp://127.0.0.1:61616";

    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Destination: 提供消息发送的目的地(Queue或者Topic)
        Destination destinationQueue = new ActiveMQQueue("queue_name");
        Destination destinationTopic = new ActiveMQTopic("topic_name");

        // 创建消息的发送者和消费者，关联到同一个Destination
        MessageProducer producer = connection.createSession().createProducer(destinationQueue);
        MessageConsumer consumer = connection.createSession().createConsumer(destinationQueue);

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText("first text message");
        producer.send(textMessage);
        System.out.println("message sent !");

        TextMessage messageReceived = (TextMessage) consumer.receiveNoWait();
        System.out.println(messageReceived.getText());
        messageReceived.acknowledge();
    }
}