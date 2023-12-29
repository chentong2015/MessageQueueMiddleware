package jndi.message.server;

import javax.jms.*;
import javax.naming.InitialContext;

public class MyReceiver {

    public static void main(String[] args) throws Exception {
        InitialContext ctx = new InitialContext();
        QueueConnectionFactory f = (QueueConnectionFactory) ctx.lookup("myQueueConnectionFactory");
        QueueConnection con = f.createQueueConnection();
        con.start();

        QueueSession ses = con.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue t = (Queue) ctx.lookup("myQueue");
        QueueReceiver receiver = ses.createReceiver(t);

        MyListener listener = new MyListener();
        receiver.setMessageListener(listener);

        // waiting for messages...
        System.out.println("press Ctrl+c to shutdown...");
        while (true) {
            Thread.sleep(1000);
        }
    }
}
