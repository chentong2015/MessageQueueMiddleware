package main.spring_messaging.components;

import java.util.concurrent.CountDownLatch;

// @Component
public class Receiver {

    // Java实现同步的组件：CountDownLatch倒计时锁存器
    // 确保一个现在在其他一个或者多个优先级线程完成之后，再执行
    private CountDownLatch latch = new CountDownLatch(1);

    // TODO: 用于测试使用，确保消息被收到了，不用在实战中
    // 实际是处理消息的回调方法
    public void receiveMessage(String message) {
        System.out.println("Received <" + message + ">");
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
