package com.jms.main;

import javax.jms.*;

public class JmsConsumer {

    public String testJmsConsumer() throws JMSException {
        ConnectionFactory defaultConnectionFactory = new MyConnectionFactory();
        try (JMSContext context = defaultConnectionFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            //NOTE: This method creates an object that encapsulates an existing topic.
            //      It does not create a new physical topic.
            Queue queue = context.createQueue("SUBSCRIPTION_NAME");

            // 在指定的Queue topic上获取信息
            JMSConsumer consumer = context.createConsumer(queue);
            final Message message = consumer.receiveNoWait();
            message.acknowledge();
            return message.getBody(String.class);
        }
    }
}
