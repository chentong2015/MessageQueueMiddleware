package com.jms.api;

import javax.jms.*;

public class MyJmsProducerConsumer {

    public void testJmsProducer() throws JMSException {
        ConnectionFactory connectionFactory = new MyConnectionFactory();
        try (JMSContext context = connectionFactory.createContext()) {
            Topic topic = context.createTopic("DESTINATION_NAME");
            TextMessage message = context.createTextMessage("message");
            message.setJMSType("#");
            // 发送的Text Message必须关联到指定的Topic
            JMSProducer producer = context.createProducer();
            producer.send(topic, message);
        }
    }

    public String testJmsConsumer() throws JMSException {
        ConnectionFactory defaultConnectionFactory = new MyConnectionFactory();
        try (JMSContext context = defaultConnectionFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            Queue queue = context.createQueue("SUBSCRIPTION_NAME");

            // 在指定的Queue topic上获取信息
            JMSConsumer consumer = context.createConsumer(queue);

            // Consumer消费完成后需要返回ack
            Message message = consumer.receiveNoWait();
            message.acknowledge();
            return message.getBody(String.class);
        }
    }
}
