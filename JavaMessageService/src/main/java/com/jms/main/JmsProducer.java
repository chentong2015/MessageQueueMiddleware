package com.jms.main;

import javax.jms.*;

public class JmsProducer {

    public void testJmsProducer() throws JMSException {
        ConnectionFactory connectionFactory = new MyConnectionFactory();
        try (JMSContext context = connectionFactory.createContext()) {
            //NOTE: This method creates an object that encapsulates an existing topic.
            //      It does not create a new physical topic.
            Topic topic = context.createTopic("DESTINATION_NAME");
            JMSProducer producer = context.createProducer();

            // 在指定的Queue Topic发送消息
            TextMessage message = context.createTextMessage("message");
            message.setJMSType("#");
            producer.send(topic, message);
        }
    }
}
