package main.spring_messaging;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MessagingRabbitmqApplication {

    public static void main(String[] args) {
        System.out.println("Starting application. OK");
        SpringApplication.run(MessagingRabbitmqApplication.class, args);
    }
}
