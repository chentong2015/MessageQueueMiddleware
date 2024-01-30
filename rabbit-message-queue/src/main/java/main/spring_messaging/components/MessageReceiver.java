package main.spring_messaging.components;

import org.springframework.stereotype.Component;

@Component
public class MessageReceiver {

    // Test: 将收到的信息显示在界面上
    public void receiveMessage(String message) {
        System.out.println("Received <" + message + "> form web controller");
    }
}
