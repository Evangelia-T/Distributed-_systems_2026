package common.map_reduce;

import java.util.Map;

public interface Reducer<K2, V2, R> {
    /**
     * Combines all intermediate values associated with the same key (reduce phase).
     */
    R reduce(Map<K2, V2> input);
}
