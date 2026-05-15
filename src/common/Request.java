package common;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Request implements Serializable {
    private final RequestType type;
    private final GameInfo gameInfo;
    private final String gameName;
    private final String providerName;
    private final String playerId;
    private final String riskLevel;
    private final String betCategory;
    private final Double minBet;
    private final Double maxBet;
    private final Double betAmount;
    private final Integer minStars;
    private final Map<String, Double> partialTotals;
    private final String requestId;
    private final Integer expectedResults;
    private final String reducerHost;
    private final Integer reducerPort;

    public Request(RequestType type, GameInfo gameInfo, String gameName, String providerName,
                   String playerId, String riskLevel, String betCategory,
                   Double minBet, Double maxBet, Double betAmount, Integer minStars,
                   Map<String, Double> partialTotals) {
        this(type, gameInfo, gameName, providerName, playerId, riskLevel, betCategory,
                minBet, maxBet, betAmount, minStars, partialTotals, null, null, null, null);
    }

    public Request(RequestType type, GameInfo gameInfo, String gameName, String providerName,
                   String playerId, String riskLevel, String betCategory,
                   Double minBet, Double maxBet, Double betAmount, Integer minStars,
                   Map<String, Double> partialTotals, String requestId, Integer expectedResults,
                   String reducerHost, Integer reducerPort) {
        this.type = type;
        this.gameInfo = gameInfo;
        this.gameName = gameName;
        this.providerName = providerName;
        this.playerId = playerId;
        this.riskLevel = riskLevel;
        this.betCategory = betCategory;
        this.minBet = minBet;
        this.maxBet = maxBet;
        this.betAmount = betAmount;
        this.minStars = minStars;
        this.partialTotals = partialTotals == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(partialTotals));
        this.requestId = requestId;
        this.expectedResults = expectedResults;
        this.reducerHost = reducerHost;
        this.reducerPort = reducerPort;
    }

    public static Request addGame(GameInfo gameInfo) {
        return new Request(RequestType.ADD_GAME, gameInfo, null, null, null, null, null, null, null, null, null, null);
    }

    public static Request removeGame(String gameName) {
        return new Request(RequestType.REMOVE_GAME, null, gameName, null, null, null, null, null, null, null, null, null);
    }

    public static Request updateGameRisk(String gameName, String riskLevel) {
        return new Request(RequestType.UPDATE_GAME_RISK, null, gameName, null, null, riskLevel, null, null, null, null, null, null);
    }

    public static Request updateGameBetLimits(String gameName, double minBet, double maxBet) {
        return new Request(RequestType.UPDATE_GAME_BET_LIMITS, null, gameName, null, null, null, null, minBet, maxBet, null, null, null);
    }

    public static Request providerStats(String providerName) {
        return new Request(RequestType.GET_PROVIDER_STATS, null, null, providerName, null, null, null, null, null, null, null, null);
    }

    public static Request playerStats(String playerId) {
        return new Request(RequestType.GET_PLAYER_STATS, null, null, null, playerId, null, null, null, null, null, null, null);
    }

    public static Request getAllGames() {
        return new Request(RequestType.GET_ALL_GAMES, null, null, null, null, null, null, null, null, null, null, null);
    }

    public static Request searchGames(String playerId, String providerName, String riskLevel, String betCategory, Integer minStars) {
        return new Request(RequestType.SEARCH_GAMES, null, null, providerName, playerId, riskLevel, betCategory, null, null, null, minStars, null);
    }

    public static Request placeBet(String playerId, String gameName, double betAmount) {
        return new Request(RequestType.PLACE_BET, null, gameName, null, playerId, null, null, null, null, betAmount, null, null);
    }

    public static Request randomNumber(String gameName) {
        return new Request(RequestType.GET_RANDOM_NUMBER, null, gameName, null, null, null, null, null, null, null, null, null);
    }

    public static Request addBalance(String playerId, double amount) {
        return new Request(RequestType.ADD_BALANCE, null, null, null, playerId, null, null, null, null, amount, null, null);
    }

    public static Request rateGame(String playerId, String gameName, int stars) {
        return new Request(RequestType.RATE_GAME, null, gameName, null, playerId, null, null, null, null, null, stars, null);
    }

    public static Request startProviderReduce(String providerName, String requestId, int expectedResults) {
        return new Request(RequestType.START_PROVIDER_REDUCE, null, null, providerName, null, null, null,
                null, null, null, null, null, requestId, expectedResults, null, null);
    }

    public static Request startPlayerReduce(String playerId, String requestId, int expectedResults) {
        return new Request(RequestType.START_PLAYER_REDUCE, null, null, null, playerId, null, null,
                null, null, null, null, null, requestId, expectedResults, null, null);
    }

    public static Request providerMapJob(String providerName, String requestId, String reducerHost, int reducerPort) {
        return new Request(RequestType.MAP_PROVIDER_STATS, null, null, providerName, null, null, null,
                null, null, null, null, null, requestId, null, reducerHost, reducerPort);
    }

    public static Request playerMapJob(String playerId, String requestId, String reducerHost, int reducerPort) {
        return new Request(RequestType.MAP_PLAYER_STATS, null, null, null, playerId, null, null,
                null, null, null, null, null, requestId, null, reducerHost, reducerPort);
    }

    public static Request providerMapPayload(String providerName, String requestId, Map<String, Double> partialTotals) {
        return new Request(RequestType.MAP_PROVIDER_STATS, null, null, providerName, null, null, null,
                null, null, null, null, partialTotals, requestId, null, null, null);
    }

    public static Request playerMapPayload(String playerId, String requestId, Map<String, Double> partialTotals) {
        return new Request(RequestType.MAP_PLAYER_STATS, null, null, null, playerId, null, null,
                null, null, null, null, partialTotals, requestId, null, null, null);
    }

    public static Request reducedResult(String requestId) {
        return new Request(RequestType.GET_REDUCED_RESULT, null, null, null, null, null, null,
                null, null, null, null, null, requestId, null, null, null);
    }

    public RequestType getType() { return type; }
    public GameInfo getGameInfo() { return gameInfo; }
    public String getGameName() { return gameName; }
    public String getProviderName() { return providerName; }
    public String getPlayerId() { return playerId; }
    public String getRiskLevel() { return riskLevel; }
    public String getBetCategory() { return betCategory; }
    public Double getMinBet() { return minBet; }
    public Double getMaxBet() { return maxBet; }
    public Double getBetAmount() { return betAmount; }
    public Integer getMinStars() { return minStars; }
    public Map<String, Double> getPartialTotals() { return partialTotals; }
    public String getRequestId() { return requestId; }
    public Integer getExpectedResults() { return expectedResults; }
    public String getReducerHost() { return reducerHost; }
    public Integer getReducerPort() { return reducerPort; }
}
