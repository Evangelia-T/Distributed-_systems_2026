package common.map_reduce;

import java.util.Map;

public interface Mapper<K1, V1, K2, V2> {
    /**
     * Processes an input (key, value) and produces intermediate key/value pairs.
     * This method represents the map phase.
     */
    Map<K2, V2> map(K1 key, V1 value);
}
