package org.veritas.training.spark.socketverse;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.stream.IntStream;

public class EventServer {

    public static final String STREAM_HOST = "localhost";
    public static final int INT_STREAM_PORT = 9999;
    public static final int STRING_STREAM_PORT = 9998;

    public EventServer() {
    }

    public static Thread getIntStreamEvents(int count) {
        return new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(INT_STREAM_PORT);
                 Socket clientSocket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)){
                IntStream.rangeClosed(1, count).forEach(i -> {
                    System.out.printf("> [%d] Posted %d%n", INT_STREAM_PORT, i);
                    out.println(i);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static Thread getStringStreamEvents(String filename) {
        return new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(STRING_STREAM_PORT);
                 Socket clientSocket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                Files.lines(new File(filename).toPath()).forEach(s -> {
                    System.out.printf("> [%d] Posted %s%n", STRING_STREAM_PORT, s);
                    out.println(s);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
