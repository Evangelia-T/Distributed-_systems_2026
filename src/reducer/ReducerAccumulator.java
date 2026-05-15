package reducer;

import common.map_reduce.Reducer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ReducerAccumulator implements Reducer<String, Double, Map<String, Double>> {
    private final Map<String, ReduceJob> jobs = new HashMap<>();

    @Override
    public Map<String, Double> reduce(Map<String, Double> partialTotals) {
        return reduce(partialTotals, true);
    }

    public Map<String, Double> reduce(Map<String, Double> partialTotals, boolean includeGrandTotal) {
        Map<String, Double> reducedTotals = new LinkedHashMap<>();
        double grandTotal = 0.0;

        for (Map.Entry<String, Double> entry : partialTotals.entrySet()) {
            reducedTotals.merge(entry.getKey(), entry.getValue(), Double::sum);
            grandTotal += entry.getValue();
        }

        if (includeGrandTotal) {
            reducedTotals.put("Total", grandTotal);
        }
        return reducedTotals;
    }

    public synchronized String startJob(String requestId, int expectedResults, String subject, boolean includeGrandTotal) {
        if (requestId == null || requestId.isBlank()) {
            return "Missing reduce request id";
        }
        if (expectedResults <= 0) {
            return "Expected results must be positive";
        }

        jobs.put(requestId, new ReduceJob(expectedResults, subject, includeGrandTotal));
        notifyAll();
        return "Reduce job started";
    }

    public synchronized String addPartial(String requestId, Map<String, Double> partialTotals) {
        ReduceJob job = jobs.get(requestId);
        if (job == null) {
            return "Unknown reduce request id: " + requestId;
        }

        for (Map.Entry<String, Double> entry : partialTotals.entrySet()) {
            job.mergedPartials.merge(entry.getKey(), entry.getValue(), Double::sum);
        }

        job.receivedResults++;
        if (job.receivedResults >= job.expectedResults) {
            job.reducedTotals = reduce(job.mergedPartials, job.includeGrandTotal);
            job.completed = true;
            notifyAll();
        }

        return "Map output accepted";
    }

    public synchronized Map<String, Double> waitForResult(String requestId) throws InterruptedException {
        while (!jobs.containsKey(requestId) || !jobs.get(requestId).completed) {
            wait();
        }

        ReduceJob job = jobs.remove(requestId);
        notifyAll();
        return job.reducedTotals;
    }

    public synchronized String getSubject(String requestId) {
        ReduceJob job = jobs.get(requestId);
        return job == null ? requestId : job.subject;
    }

    private static class ReduceJob {
        private final int expectedResults;
        private final String subject;
        private final boolean includeGrandTotal;
        private final Map<String, Double> mergedPartials = new LinkedHashMap<>();
        private int receivedResults;
        private boolean completed;
        private Map<String, Double> reducedTotals = new LinkedHashMap<>();

        private ReduceJob(int expectedResults, String subject, boolean includeGrandTotal) {
            this.expectedResults = expectedResults;
            this.subject = subject;
            this.includeGrandTotal = includeGrandTotal;
        }
    }
}
