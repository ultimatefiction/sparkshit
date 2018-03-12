package org.veritas.training.spark.scrapped;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class App {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) throws InterruptedException {
        // Configure and initialize the SparkStreamingContext
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("VerySimpleStreamingApp");
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(conf, Durations.seconds(5));
        Logger.getRootLogger().setLevel(Level.ERROR);

        // Receive streaming data from the source
        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);
        lines.print();
        System.out.println("=======");
        int result = 0;
        lines
                .map((Function<String, Integer>) Integer::parseInt)
                .filter((Function<Integer, Boolean>) integer -> integer % 2 == 0)
                .reduce((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2)
                .print();

        // Execute the Spark workflow defined above
        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
