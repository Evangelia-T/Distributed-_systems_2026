package master;

import common.GameInfo;
import common.Response;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CasinoState {
    private final Map<String, GameInfo> gamesByName = new LinkedHashMap<>();
    private final Map<String, Double> gameProfitLoss = new LinkedHashMap<>();

    public synchronized Response addGame(GameInfo gameInfo) {
        if (gameInfo == null || gameInfo.getGameName() == null || gameInfo.getGameName().isBlank()) {
            return new Response(false, "Missing game info");
        }
        gamesByName.put(gameInfo.getGameName(), gameInfo);
        gameProfitLoss.putIfAbsent(gameInfo.getGameName(), 0.0);
        return new Response(true, "Game added/updated: " + gameInfo.getGameName());
    }

    public synchronized Response removeGame(String gameName) {
        GameInfo removed = gamesByName.remove(gameName);
        return removed == null
                ? new Response(false, "Game not found: " + gameName)
                : new Response(true, "Removed game: " + gameName);
    }

    public synchronized Response updateRisk(String gameName, String riskLevel) {
        GameInfo gameInfo = gamesByName.get(gameName);
        if (gameInfo == null) return new Response(false, "Game not found: " + gameName);
        gamesByName.put(gameName, gameInfo.withRiskLevel(riskLevel));
        return new Response(true, "Risk updated for " + gameName);
    }

    public synchronized Response updateBetLimits(String gameName, double minBet, double maxBet) {
        GameInfo gameInfo = gamesByName.get(gameName);
        if (gameInfo == null) return new Response(false, "Game not found: " + gameName);

        GameInfo updated = new GameInfo(gameInfo.getGameName(), gameInfo.getProviderName(), gameInfo.getStars(),
                gameInfo.getNoOfVotes(), gameInfo.getGameLogo(), minBet, maxBet, gameInfo.getRiskLevel(), gameInfo.getHashKey());
        gamesByName.put(gameName, updated);
        return new Response(true, "Bet limits updated for " + gameName);
    }

    public synchronized Response getAllAvailableGames() {
        return new Response(true, "All available games", new ArrayList<>(gamesByName.values()));
    }

    public synchronized Response search(String providerName, String riskLevel, String betCategory, Integer minStars) {
        List<GameInfo> filtered = new ArrayList<>();
        for (GameInfo g : gamesByName.values()) {
            if (providerName != null && !providerName.isBlank() && !providerName.equalsIgnoreCase(g.getProviderName()))
                continue;
            if (riskLevel != null && !riskLevel.isBlank() && !riskLevel.equalsIgnoreCase(g.getRiskLevel())) continue;
            if (betCategory != null && !betCategory.isBlank() && !betCategory.equals(g.getBetCategory())) continue;
            if (minStars != null && g.getStars() < minStars) continue;
            filtered.add(g);
        }
        return new Response(true, "Found " + filtered.size() + " games", filtered);
    }
}
