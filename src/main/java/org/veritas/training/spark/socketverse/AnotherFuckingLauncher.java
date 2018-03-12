package org.veritas.training.spark.socketverse;

public class AnotherFuckingLauncher {

    public static void main(String[] args) {

        final int COUNT = 100;
        final String PATH = "D:\\spark\\src\\main\\resources\\file.txt";

        EventServer.getIntStreamEvents(COUNT).start();
        EventServer.getStringStreamEvents(PATH).start();

        //Thread.sleep(1500);

        StreamReader.getIntReader(EventServer.STREAM_HOST, EventServer.INT_STREAM_PORT).start();
        StreamReader.getStringReader(EventServer.STREAM_HOST, EventServer.STRING_STREAM_PORT).start();

    }

}
