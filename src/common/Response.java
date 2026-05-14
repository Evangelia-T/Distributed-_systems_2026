package common;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Response implements Serializable {
    private final boolean success;
    private final String message;
    private final Map<String, Double> totals;
    private final List<GameInfo> games;

    public Response(boolean success, String message) {
        this(success, message, Collections.emptyMap(), Collections.emptyList());
    }

    public Response(boolean success, String message, Map<String, Double> totals) {
        this(success, message, totals, Collections.emptyList());
    }

    public Response(boolean success, String message, List<GameInfo> games) {
        this(success, message, Collections.emptyMap(), games);
    }

    public Response(boolean success, String message, Map<String, Double> totals, List<GameInfo> games) {
        this.success = success;
        this.message = message;
        this.totals = totals == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(totals));
        this.games = games == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(games);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, Double> getTotals() {
        return totals;
    }

    public List<GameInfo> getGames() {
        return games;
    }
}
