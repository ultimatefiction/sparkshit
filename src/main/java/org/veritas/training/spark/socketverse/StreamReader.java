package org.veritas.training.spark.socketverse;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.veritas.training.spark.RedissonClientProvider;
import org.veritas.training.spark.StreamingContextProvider;

public class StreamReader {

    public StreamReader() {
    }

    public static Thread getIntReader(String host, int port) {
        return new Thread(() -> {
            JavaStreamingContext streamingContext = StreamingContextProvider.getContext();
            JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(host, port);

            lines.print();
            lines
                    .map((Function<String, Integer>) Integer::parseInt)
                    .filter((Function<Integer, Boolean>) integer -> integer % 2 == 0)
                    .reduce((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2)
                    .print();

            streamingContext.start();
            streamingContext.awaitTermination();
        });
    }

    public static Thread getStringReader(String host, int port) {
        return new Thread(() -> {
            JavaStreamingContext streamingContext = StreamingContextProvider.getContext();
            JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(host, port);
            RedissonClient client = RedissonClientProvider.getClient();
            RScoredSortedSet<String> wordSet = client.getScoredSortedSet("spark_words");

            lines.print();
            lines.foreachRDD((Function<JavaRDD<String>, Void>) stringJavaRDD -> {
                String word = stringJavaRDD.first();
                if (word.contains(word)) {
                    wordSet.addScore(word, wordSet.getScore(word) + 1);
                } else {
                    wordSet.add(1, word);
                }
                return null;
            });

            streamingContext.start();
            streamingContext.awaitTermination();
        });
    }

}
