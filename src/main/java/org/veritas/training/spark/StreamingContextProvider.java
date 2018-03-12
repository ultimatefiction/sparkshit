package org.veritas.training.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingContextProvider {

    public static final String MASTER = "local[*]";
    public static final String APP_NAME = "VerySimpleStreamingApp";

    public static JavaStreamingContext getContext() {

        SparkConf conf = new SparkConf()
                .setMaster(MASTER)
                .setAppName(APP_NAME);
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(conf, Durations.seconds(5));
        Logger.getRootLogger().setLevel(Level.ERROR);

        return streamingContext;

    }

}
