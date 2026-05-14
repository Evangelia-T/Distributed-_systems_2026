package reducer;

import common.Request;
import common.RequestType;
import common.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

public class ReducerRequestHandler implements Runnable {
    private final Socket socket;
    private final ReducerAccumulator accumulator;

    public ReducerRequestHandler(Socket socket, ReducerAccumulator accumulator) {
        this.socket = socket;
        this.accumulator = accumulator;
    }

    @Override
    public void run() {
        try (Socket client = socket;
             ObjectInputStream in = new ObjectInputStream(client.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream())) {

            Request request = (Request) in.readObject();
            Response response = handle(request);
            out.writeObject(response);
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Response handle(Request request) {
        if (request == null || request.getType() == null) {
            return new Response(false, "Invalid reducer request");
        }

        if (request.getType() != RequestType.MAP_PROVIDER_STATS && request.getType() != RequestType.MAP_PLAYER_STATS) {
            return new Response(false, "Reducer only supports map outputs");
        }

        Map<String, Double> totals = accumulator.reduce(request.getPartialTotals());
        String subject = request.getType() == RequestType.MAP_PROVIDER_STATS
                ? request.getProviderName()
                : request.getPlayerId();

        return new Response(true, "Reduced totals for " + subject, totals);
    }
}
