package reducer;

import common.map_reduce.Reducer;

import java.util.LinkedHashMap;
import java.util.Map;

public class ReducerAccumulator implements Reducer<String, Double, Map<String, Double>> {

    @Override
    public Map<String, Double> reduce(Map<String, Double> partialTotals) {
        Map<String, Double> reducedTotals = new LinkedHashMap<>();
        double grandTotal = 0.0;

        for (Map.Entry<String, Double> entry : partialTotals.entrySet()) {
            reducedTotals.merge(entry.getKey(), entry.getValue(), Double::sum);
            grandTotal += entry.getValue();
        }

        reducedTotals.put("Total", grandTotal);
        return reducedTotals;
    }
}
