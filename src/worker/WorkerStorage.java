package worker;

import common.map_reduce.Mapper;

import java.util.HashMap;
import java.util.Map;

public class WorkerStorage implements Mapper<String, String, String, Double> {

    private HashMap<String, Game> games;
    private HashMap<String, Double> playerBalances;
    private HashMap<String, Double> playerProfitLoss;

    public WorkerStorage() {
        games = new HashMap<>();
        playerBalances = new HashMap<>();
        playerProfitLoss = new HashMap<>();
    }


    @Override
    public Map<String, Double> map(String key, String value) {
        if (key == null || value == null) return new HashMap<>();
        if (key.equals("provider")) {
            return getProviderPartialTotals(value);
        } else if (key.equals("player")) {
            return getPlayerPartialTotals(value);
        } else {
            return new HashMap<>();
        }
    }

    public synchronized String addGame(Game game) {
        if (game == null) {
            return "Game is null";
        }

        String gameName = game.getGameName();

        if (gameName == null || gameName.trim().isEmpty()) {
            return "Game name is empty";
        }

        Game existingGame = games.get(gameName);
        if (existingGame != null) {
            if (existingGame.isActive()) {
                return "Game already exists";
            }

            existingGame.setActive(true);
            existingGame.setRiskLevel(game.getRiskLevel());
            existingGame.setMinBet(game.getMinBet());
            existingGame.setMaxBet(game.getMaxBet());
            return "Game re-activated successfully";

        }

        games.put(gameName, game);
        return "Game added successfully";
    }


    public synchronized String removeGame(String gameName) {
        if (gameName == null || gameName.trim().isEmpty()) {
            return "Game name is empty";
        }

        Game game = games.get(gameName);

        if (game == null) {
            return "Game not found";
        }

        game.setActive(false);
        return "Game removed successfully (set inactive)";
    }

    public synchronized String updateRisk(String gameName, String newRisk) {
        if (gameName == null || gameName.trim().isEmpty()) {
            return "Game name is empty";
        }

        if (newRisk == null || newRisk.trim().isEmpty()) {
            return "Risk level is empty";
        }

        Game game = games.get(gameName);

        if (game == null) {
            return "Game not found";
        }

        game.setRiskLevel(newRisk);
        return "Risk updated successfully";
    }

    public synchronized String updateBetLimits(String gameName, Double newMinBet, Double newMaxBet) {
        if (gameName == null || gameName.trim().isEmpty()) {
            return "Game name is empty";
        }

        if (newMinBet == null || newMaxBet == null) {
            return "Bet limits are null";
        }

        if (newMinBet <= 0 || newMaxBet <= 0) {
            return "Bet limits must be positive";
        }

        if (newMinBet > newMaxBet) {
            return "Min bet cannot be greater than max bet";
        }

        Game game = games.get(gameName);

        if (game == null) {
            return "Game not found";
        }

        game.setMinBet(newMinBet);
        game.setMaxBet(newMaxBet);

        return "Bet limits updated successfully";
    }

    public synchronized Game getGame(String gameName) {
        return games.get(gameName);
    }

    public synchronized HashMap<String, Game> getAllGames() {
        return new HashMap<>(games);
    }

    public synchronized HashMap<String, Double> getProviderPartialTotals(String providerName) {
        HashMap<String, Double> partialTotals = new HashMap<>();

        if (providerName == null || providerName.trim().isEmpty()) {
            return partialTotals;
        }

        for (Game game : games.values()) {
            if (providerName.equalsIgnoreCase(game.getProviderName())) {
                double profitLoss = game.getTotalBetAmount() - game.getTotalPayoutAmount();
                partialTotals.put(game.getGameName(), profitLoss);
            }
        }

        return partialTotals;

    }

    public synchronized String addBalance(String playerId, Double amount) {
        if (playerId == null || playerId.trim().isEmpty()) {
            return "Player ID is empty";
        }

        if (amount == null) {
            return "Amount is null";
        }

        if (amount <= 0) {
            return "Amount must be positive";
        }

        double currentBalance = playerBalances.getOrDefault(playerId, 0.0);
        playerBalances.put(playerId, currentBalance + amount);

        return "Balance added successfully";
    }

    public synchronized boolean deductBalance(String playerId, Double amount) {
        if (playerId == null || playerId.trim().isEmpty() || amount == null || amount <= 0) {
            return false;
        }

        double currentBalance = playerBalances.getOrDefault(playerId, 0.0);

        if (currentBalance < amount) {
            return false;
        }

        playerBalances.put(playerId, currentBalance - amount);
        return true;
    }

    public synchronized void addWinnings(String playerId, Double payout) {
        if (playerId == null || playerId.trim().isEmpty() || payout == null || payout < 0) {
            return;
        }

        double currentBalance = playerBalances.getOrDefault(playerId, 0.0);
        playerBalances.put(playerId, currentBalance + payout);
    }


    public synchronized void updatePlayerProfitLoss(String playerId, double netAmount) {
        if (playerId == null || playerId.trim().isEmpty()) {
            return;
        }

        double current = playerProfitLoss.getOrDefault(playerId, 0.0);
        playerProfitLoss.put(playerId, current + netAmount);
    }

    public synchronized HashMap<String, Double> getPlayerPartialTotals(String playerId) {
        HashMap<String, Double> partialTotals = new HashMap<>();

        if (playerId == null || playerId.trim().isEmpty()) {
            return partialTotals;
        }

        partialTotals.put("Total Profit/Loss", playerProfitLoss.getOrDefault(playerId, 0.0));
        return partialTotals;
    }
}