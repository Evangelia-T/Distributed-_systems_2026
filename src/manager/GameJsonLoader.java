package manager;

import common.GameInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class GameJsonLoader {
    private static final Pattern ENTRY_PATTERN = Pattern.compile("\"([^\"]+)\"\\s*:\\s*(\"([^\"]*)\"|-?\\d+(?:\\.\\d+)?)");

    private GameJsonLoader() {
    }

    public static GameInfo loadFromPath(String jsonPath) throws IOException {
        String content = Files.readString(Path.of(jsonPath));
        Map<String, String> values = parseFlatJson(content);

        return new GameInfo(
                required(values, "GameName"),
                required(values, "ProviderName"),
                parseInt(required(values, "Stars"), "Stars"),
                parseInt(required(values, "NoOfVotes"), "NoOfVotes"),
                required(values, "GameLogo"),
                parseDouble(required(values, "MinBet"), "MinBet"),
                parseDouble(required(values, "MaxBet"), "MaxBet"),
                required(values, "RiskLevel"),
                required(values, "HashKey")
        );
    }

    private static Map<String, String> parseFlatJson(String content) {
        Matcher matcher = ENTRY_PATTERN.matcher(content);
        Map<String, String> values = new LinkedHashMap<>();

        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(3) != null ? matcher.group(3) : matcher.group(2);
            values.put(key, value);
        }

        return values;
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required key in JSON: " + key);
        }
        return value;
    }

    private static int parseInt(String value, String key) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for " + key + ": " + value, e);
        }
    }

    private static double parseDouble(String value, String key) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid decimal value for " + key + ": " + value, e);
        }
    }
}