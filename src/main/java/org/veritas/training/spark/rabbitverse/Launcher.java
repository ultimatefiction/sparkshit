package org.veritas.training.spark.rabbitverse;

public class Launcher {

    public static void main(String[] args) {

        final int MAX_MESSAGES = 10;
        final int COOLDOWN_TIME = 1000;
        final String EXCHANGE_NAME = "rabbit_to_spark";

        new MessageGenerator(MAX_MESSAGES, COOLDOWN_TIME, EXCHANGE_NAME).start();

    }

}
