package reducer;

import java.net.ServerSocket;
import java.net.Socket;

public class ReducerServer {
    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
        ReducerAccumulator accumulator = new ReducerAccumulator();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Reducer started on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                Thread thread = new Thread(new ReducerRequestHandler(socket, accumulator));
                thread.start();
            }
        }
    }
}
