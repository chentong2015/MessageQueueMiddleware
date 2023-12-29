package apache.activemq.main.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MessageTopicConsumer {

    private static final String URL = "http://127.0.0.1:61616";
    private static final String TOPIC_NAME = "topic name";

    public static void main(String[] args) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = session.createTopic(TOPIC_NAME);
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(message -> {
            TextMessage textMessage = (TextMessage) message;
            try {
                System.out.println("Received：" + textMessage.getText());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
    }
}

