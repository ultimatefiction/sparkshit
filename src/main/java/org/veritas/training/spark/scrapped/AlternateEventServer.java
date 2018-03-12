package org.veritas.training.spark.scrapped;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.IntStream;

public class AlternateEventServer {

    private static final int PORT = 9999;

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(PORT);
        Socket clientSocket = serverSocket.accept();
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        IntStream.rangeClosed(1, 10).forEach(i -> {
            System.out.printf("> Posted %d%n", i);
            out.println(i);
        });

    }

}
