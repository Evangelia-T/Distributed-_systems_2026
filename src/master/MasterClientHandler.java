package master;

import common.Request;
import common.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class MasterClientHandler implements Runnable {
    private final Socket clientSocket;
    private final MasterDispatcher dispatcher;

    public MasterClientHandler(Socket clientSocket, MasterDispatcher dispatcher) {
        this.clientSocket = clientSocket;
        this.dispatcher = dispatcher;
    }

    @Override
    public void run() {
        try (Socket socket = clientSocket;
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            Request request = (Request) in.readObject();
            Response response = dispatcher.dispatch(request);

            out.writeObject(response);
            out.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}