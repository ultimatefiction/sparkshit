package org.veritas.training.spark.rabbitverse;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

public class MessageGenerator extends Thread {

    private int maxMessages;
    private int coolDownTime;
    private String exchangeName;
    private Channel channel;

    public MessageGenerator(int maxMessages, int coolDownTime, String exchangeName) {
        this.maxMessages = maxMessages;
        this.coolDownTime = coolDownTime;
        this.exchangeName = exchangeName;
    }

    private void send(String message) {
        try {
            channel.basicPublish(exchangeName, "", null, message.getBytes());
            System.out.printf("[>] Published '%s' to rabbit exchange%n", message);
            Thread.sleep(coolDownTime);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            channel = new ChannelProvider().getChannel();
            channel.exchangeDeclare(exchangeName, "fanout");
            IntStream.rangeClosed(1, maxMessages).forEach(x -> send(String.format("Kek %d", x)));
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }

}
