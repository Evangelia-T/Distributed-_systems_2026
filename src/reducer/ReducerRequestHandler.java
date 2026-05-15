package reducer;

import common.Request;
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

        return switch (request.getType()) {
            case START_PROVIDER_REDUCE -> startReduceJob(
                    request.getRequestId(), request.getExpectedResults(), request.getProviderName(), true);
            case START_PLAYER_REDUCE -> startReduceJob(
                    request.getRequestId(), request.getExpectedResults(), request.getPlayerId(), false);
            case MAP_PROVIDER_STATS, MAP_PLAYER_STATS -> acceptMapOutput(request);
            case GET_REDUCED_RESULT -> waitForReducedResult(request.getRequestId());
            default -> new Response(false, "Unsupported reducer request type: " + request.getType());
        };
    }

    private Response startReduceJob(String requestId, Integer expectedResults, String subject, boolean includeGrandTotal) {
        if (expectedResults == null) {
            return new Response(false, "Missing expected result count");
        }

        String result = accumulator.startJob(requestId, expectedResults, subject, includeGrandTotal);
        if ("Reduce job started".equals(result)) {
            return new Response(true, result);
        }
        return new Response(false, result);
    }

    private Response acceptMapOutput(Request request) {
        String result = accumulator.addPartial(request.getRequestId(), request.getPartialTotals());
        if ("Map output accepted".equals(result)) {
            return new Response(true, result);
        }
        return new Response(false, result);
    }

    private Response waitForReducedResult(String requestId) {
        try {
            String subject = accumulator.getSubject(requestId);
            Map<String, Double> totals = accumulator.waitForResult(requestId);
            return new Response(true, "Reduced totals for " + subject, totals);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Response(false, "Interrupted while waiting for reduced result");
        }
    }
}
