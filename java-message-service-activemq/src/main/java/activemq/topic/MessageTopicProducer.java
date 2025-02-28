package activemq.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MessageTopicProducer {

    private static final String URL = "http://127.0.0.1:61616";
    private static final String TOPIC_NAME = "topic name";

    public static void main(String[] args) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = session.createTopic(TOPIC_NAME);
        MessageProducer producer = session.createProducer(destination);

        for (int i = 0; i < 100; i++) {
            TextMessage textMessage = session.createTextMessage("test queue message" + i);
            producer.send(textMessage);
            System.out.println("Send：" + textMessage.getText());
        }
        session.close();
    }
}
