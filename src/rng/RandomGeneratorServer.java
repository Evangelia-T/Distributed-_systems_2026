package rng;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class RandomGeneratorServer {

    public static void main(String[] args) {
        ServerSocket server = null;

        try {
            int port = 9090;
            server = new ServerSocket(port);
            System.out.println("RandomGeneratorServer started on port " + port);

            while (true) {
                Socket client = server.accept();
                RandomRequestHandler handler = new RandomRequestHandler(client);
                handler.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (server != null && !server.isClosed()) {
                    server.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}