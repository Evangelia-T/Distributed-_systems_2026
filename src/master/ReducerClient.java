package master;

import common.Request;
import common.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ReducerClient {
    private final String reducerHost;
    private final int reducerPort;

    public ReducerClient(String reducerHost, int reducerPort) {
        this.reducerHost = reducerHost;
        this.reducerPort = reducerPort;
    }

    public String getReducerHost() {
        return reducerHost;
    }

    public int getReducerPort() {
        return reducerPort;
    }

    public Response startReduce(Request request) {
        return send(request);
    }

    public Response waitForResult(String requestId) {
        return send(Request.reducedResult(requestId));
    }

    private Response send(Request request) {
        try (Socket socket = new Socket(reducerHost, reducerPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(request);
            out.flush();

            return (Response) in.readObject();
        } catch (Exception e) {
            return new Response(false, "Failed to communicate with reducer: " + e.getMessage());
        }
    }
}
