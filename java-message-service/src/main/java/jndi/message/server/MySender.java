package jndi.message.server;

import javax.jms.*;
import javax.naming.InitialContext;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MySender {

    // 创建Message Server后使用JDNI接口并通过特定的名称找到QueueConnectionFactory
    // 然后在指定的Destination中发布和接收消息
    public static void main(String[] args) throws Exception {
        InitialContext ctx = new InitialContext();
        QueueConnectionFactory f = (QueueConnectionFactory) ctx.lookup("myQueueConnectionFactory");
        QueueConnection con = f.createQueueConnection();
        con.start();

        QueueSession ses = con.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue t = (Queue) ctx.lookup("myQueue");
        QueueSender sender = ses.createSender(t);

        TextMessage msg = ses.createTextMessage();
        BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.println("Enter Msg, end to terminate:");
            String s = b.readLine();
            if (s.equals("end"))
                break;
            msg.setText(s);
            sender.send(msg);
            System.out.println("Message successfully sent.");
        }
        con.close();
    }
}
