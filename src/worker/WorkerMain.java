package worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class WorkerMain {

    public static void main(String[] args) {
        WorkerStorage storage = new WorkerStorage();
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8081;

        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("Worker server started at port " + port);


            while (true) {
                Socket client = server.accept();
                System.out.println("New connection from: " + client.getInetAddress());

                HandleThreadWorker thread = new HandleThreadWorker(client, storage);
                thread.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}