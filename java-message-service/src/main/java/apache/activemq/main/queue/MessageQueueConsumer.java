package apache.activemq.main.queue;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MessageQueueConsumer {

    private static final String URL = "http://127.0.0.1:61616";
    private static final String QUEUE_NAME = "queue-test";

    public static void main(String[] args) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = session.createQueue(QUEUE_NAME);
        javax.jms.MessageConsumer consumer = session.createConsumer(destination);

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
