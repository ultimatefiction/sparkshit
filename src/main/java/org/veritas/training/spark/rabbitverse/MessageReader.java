package org.veritas.training.spark.rabbitverse;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Map;

public class MessageReader extends Thread {


    @Override
    public void run() {
        try {
            System.out.println("[X] Creating Spark Configuration");
            SparkConf conf = new SparkConf();
            conf.setAppName("RabbitMq Receiver");
            conf.setMaster("local[2]");

            System.out.println("[X] Retrieving Streaming Context from SparkConf");
            JavaStreamingContext streamCtx = new JavaStreamingContext(conf, Durations.seconds(2));

            Map<String, String> rabbitMqConParams = new HashMap<>();
            rabbitMqConParams.put("host", "localhost");
            rabbitMqConParams.put("exchangeName", "rabbit_to_spark");
            rabbitMqConParams.put("userName", "msxfusr");
            rabbitMqConParams.put("password", "msxfpwd");
            System.out.println("Trying to connect to RabbitMq");

            //JavaReceiverInputDStream receiverStream = RabbitMQUtils.
            //<String> receiverStream = RabbitMQUtils.createJavaStreamFromAQueue(streamCtx, rabbitMqConParams);

            //receiverStream.foreachRDD((Function<JavaRDD<String>, Void>) arg0 -> {
            //    System.out.println("Value Received " + arg0.toString());
            //    return null;
            //});
            //streamCtx.start();
            //streamCtx.awaitTermination();
        } catch (Exception e) {

        }
    }

}
