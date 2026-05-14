package worker;

import common.GameInfo;
import common.Request;
import common.RequestType;
import common.Response;
import rng.RandomResult;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class HandleThreadWorker extends Thread {

    private final Socket client;
    private final WorkerStorage storage;

    // RNG server config
    private static final String RNG_HOST = "localhost";
    private static final int RNG_PORT = 9090;

    public HandleThreadWorker(Socket client, WorkerStorage storage) {
        this.client = client;
        this.storage = storage;
    }

    @Override
    public void run() {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        try {
            out = new ObjectOutputStream(client.getOutputStream());
            out.flush();

            in = new ObjectInputStream(client.getInputStream());

            Request request = (Request) in.readObject();
            Response response = handleRequest(request);

            out.writeObject(response);
            out.flush();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (out != null) out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (client != null && !client.isClosed()) {
                    client.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Response handleRequest(Request request) {
        if (request == null) {
            return new Response(false, "Request is null");
        }

        RequestType type = request.getType();

        if (type == null) {
            return new Response(false, "Request type is null");
        }

        switch (type) {
            case ADD_GAME:
                return handleAddGame(request.getGameInfo());

            case REMOVE_GAME:
                return handleRemoveGame(request.getGameName());

            case UPDATE_GAME_RISK:
                return handleUpdateRisk(request.getGameName(), request.getRiskLevel());

            case UPDATE_GAME_BET_LIMITS:
                return handleUpdateBetLimits(request.getGameName(), request.getMinBet(), request.getMaxBet());

            case SEARCH_GAMES:
                return handleSearchGames(request);

            case PLACE_BET:
                return handlePlaceBet(request.getPlayerId(), request.getGameName(), request.getBetAmount());

            case ADD_BALANCE:
                return handleAddBalance(request.getPlayerId(), request.getBetAmount());

            case GET_PROVIDER_STATS:
                return handleGetProviderStats(request.getProviderName());
            case GET_PLAYER_STATS:
                return handleGetPlayerStats(request.getPlayerId());
            case MAP_PROVIDER_STATS:
                return handleMapProviderStats(request);

            case MAP_PLAYER_STATS:
                return handleMapPlayerStats(request);
            default:
                return new Response(false, "Unsupported request type: " + type);
        }
    }

    private Response handleAddGame(GameInfo gameInfo) {
        if (gameInfo == null) {
            return new Response(false, "GameInfo is null");
        }

        if (gameInfo.getGameName() == null || gameInfo.getGameName().trim().isEmpty()) {
            return new Response(false, "Game name is empty");
        }

        Game game = new Game(gameInfo);
        String result = storage.addGame(game);

        if ("Game added successfully".equals(result) || "Game re-activated successfully".equals(result)) {
            Response rngResponse = sendRequestToRandomGenerator(Request.addGame(gameInfo));
            if (!rngResponse.isSuccess()) {
                storage.removeGame(gameInfo.getGameName());
                return rngResponse;
            }

            return new Response(true, result + " and RNG queue registered");
        }

        return new Response(false, result);
    }

    private Response handleRemoveGame(String gameName) {
        if (gameName == null || gameName.trim().isEmpty()) {
            return new Response(false, "Game name is empty");
        }

        String result = storage.removeGame(gameName);

        if ("Game removed successfully (set inactive)".equals(result)) {
            Response rngResponse = sendRequestToRandomGenerator(Request.removeGame(gameName));
            if (!rngResponse.isSuccess()) {
                return rngResponse;
            }

            return new Response(true, result + " and RNG queue removed");
        }

        return new Response(false, result);
    }

    private Response handleUpdateRisk(String gameName, String newRisk) {
        if (gameName == null || gameName.trim().isEmpty()) {
            return new Response(false, "Game name is empty");
        }

        if (newRisk == null || newRisk.trim().isEmpty()) {
            return new Response(false, "Risk level is empty");
        }

        String result = storage.updateRisk(gameName, newRisk);

        if ("Risk updated successfully".equals(result)) {
            return new Response(true, result);
        }

        return new Response(false, result);
    }

    private Response handleUpdateBetLimits(String gameName, Double minBet, Double maxBet) {
        if (gameName == null || gameName.trim().isEmpty()) {
            return new Response(false, "Game name is empty");
        }

        String result = storage.updateBetLimits(gameName, minBet, maxBet);

        if ("Bet limits updated successfully".equals(result)) {
            return new Response(true, result);
        }

        return new Response(false, result);
    }

    private Response handleGetProviderStats(String providerName) {
        if (providerName == null || providerName.trim().isEmpty()) {
            return new Response(false, "Provider name is empty");
        }

        return new Response(
                true,
                "Provider partial totals ready",
                storage.getProviderPartialTotals(providerName)
        );
    }

    private Response handleSearchGames(Request request) {
        List<GameInfo> results = new ArrayList<>();

        for (Game game : storage.getAllGames().values()) {
            if (!game.isActive()) {
                continue;
            }

            if (!matchesProvider(game, request.getProviderName())) {
                continue;
            }

            if (!matchesRisk(game, request.getRiskLevel())) {
                continue;
            }

            if (!matchesStars(game, request.getMinStars())) {
                continue;
            }

            if (!matchesBetCategory(game, request.getBetCategory())) {
                continue;
            }

            results.add(convertToGameInfo(game));
        }

        return new Response(true, "Search results", results);
    }

    private Response handlePlaceBet(String playerId, String gameName, Double betAmount) {
        if (playerId == null || playerId.trim().isEmpty()) {
            return new Response(false, "Player ID is empty");
        }

        if (gameName == null || gameName.trim().isEmpty()) {
            return new Response(false, "Game name is empty");
        }

        if (betAmount == null) {
            return new Response(false, "Bet amount is null");
        }

        if (betAmount <= 0) {
            return new Response(false, "Bet amount must be positive");
        }

        Game game = storage.getGame(gameName);

        if (game == null) {
            return new Response(false, "Game not found");
        }

        if (!game.isActive()) {
            return new Response(false, "Game is inactive");
        }

        if (betAmount < game.getMinBet() || betAmount > game.getMaxBet()) {
            return new Response(false, "Bet amount is outside allowed range");
        }

        boolean deducted = storage.deductBalance(playerId, betAmount);
        if (!deducted) {
            return new Response(false, "Insufficient balance");
        }

        double payout;
        try {
            payout = calculatePayout(game, betAmount);
        } catch (RuntimeException e) {
            storage.addWinnings(playerId, betAmount);
            return new Response(false, e.getMessage());
        }

        synchronized (game) {
            game.addToTotalBetAmount(betAmount);
            game.addToTotalPayoutAmount(payout);
        }

        storage.addWinnings(playerId, payout);
        storage.updatePlayerProfitLoss(playerId, payout - betAmount);

        String message = "Play successful. Bet: " + betAmount + ", Payout: " + payout;
        return new Response(true, message);
    }

    private Response handleAddBalance(String playerId, Double amount) {
        if (playerId == null || playerId.trim().isEmpty()) {
            return new Response(false, "Player ID is empty");
        }

        String result = storage.addBalance(playerId, amount);

        if ("Balance added successfully".equals(result)) {
            return new Response(true, result);
        }

        return new Response(false, result);
    }

    private double calculatePayout(Game game, double betAmount) {
        int randomNumber = getVerifiedRandomNumber(game);
        int mod100 = Math.floorMod(randomNumber, 100);

        if (mod100 == 0) {
            return betAmount * game.getJackpot();
        }

        int mod10 = Math.floorMod(randomNumber, 10);
        double multiplier = switch (game.getRiskLevel().toLowerCase()) {
            case "high" -> new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 6.5}[mod10];
            case "medium" -> new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 1.5, 2.5, 3.5}[mod10];
            default -> new double[]{0.0, 0.0, 0.0, 0.1, 0.5, 1.0, 1.1, 1.3, 2.0, 2.5}[mod10];
        };

        return betAmount * multiplier;
    }

    private int getVerifiedRandomNumber(Game game) {
        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            socket = new Socket(RNG_HOST, RNG_PORT);

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            in = new ObjectInputStream(socket.getInputStream());

            out.writeObject(Request.randomNumber(game.getGameName()));
            out.flush();

            RandomResult result = (RandomResult) in.readObject();

            if (result.getRandomNumber() < 0) {
                throw new RuntimeException(result.getHash());
            }

            String expectedHash = sha256(result.getRandomNumber() + game.getHashKey());

            if (!expectedHash.equals(result.getHash())) {
                throw new RuntimeException("Hash verification failed");
            }

            return result.getRandomNumber();

        } catch (Exception e) {
            throw new RuntimeException("Failed to get verified random number from RNG server", e);
        } finally {
            try {
                if (in != null) in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (out != null) out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Response sendRequestToRandomGenerator(Request request) {
        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            socket = new Socket(RNG_HOST, RNG_PORT);

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            in = new ObjectInputStream(socket.getInputStream());

            out.writeObject(request);
            out.flush();

            RandomResult result = (RandomResult) in.readObject();
            if (result.getRandomNumber() < 0) {
                return new Response(false, "RNG failed: " + result.getHash());
            }

            return new Response(true, result.getHash());

        } catch (Exception e) {
            return new Response(false, "Failed to communicate with RNG server: " + e.getMessage());
        } finally {
            try {
                if (in != null) in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (out != null) out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encoded = digest.digest(input.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder();
            for (byte b : encoded) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            return hexString.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    private boolean matchesProvider(Game game, String providerName) {
        if (providerName == null || providerName.trim().isEmpty()) {
            return true;
        }

        return providerName.equalsIgnoreCase(game.getProviderName());
    }

    private boolean matchesRisk(Game game, String riskLevel) {
        if (riskLevel == null || riskLevel.trim().isEmpty()) {
            return true;
        }

        return riskLevel.equalsIgnoreCase(game.getRiskLevel());
    }

    private boolean matchesStars(Game game, Integer minStars) {
        if (minStars == null) {
            return true;
        }

        return game.getStars() >= minStars;
    }

    private boolean matchesBetCategory(Game game, String betCategory) {
        if (betCategory == null || betCategory.trim().isEmpty()) {
            return true;
        }

        String gameCategory = calculateBetCategory(game.getMinBet());
        return betCategory.equalsIgnoreCase(gameCategory);
    }

    private String calculateBetCategory(double minBet) {
        if (minBet >= 5.0) {
            return "$$$";
        } else if (minBet >= 1.0) {
            return "$$";
        } else {
            return "$";
        }
    }

    private Response handleGetPlayerStats(String playerId) {
        if (playerId == null || playerId.trim().isEmpty()) {
            return new Response(false, "Player ID is empty");
        }

        return new Response(
                true,
                "Player partial totals ready",
                storage.getPlayerPartialTotals(playerId)
        );
    }

    private Response handleMapProviderStats(Request request) {
        String providerName = request.getProviderName();
        if (providerName == null || providerName.trim().isEmpty()) {
            return new Response(false, "Provider name is empty");
        }

        if (request.getRequestId() == null || request.getRequestId().isBlank()) {
            return new Response(true, "Provider map output ready", storage.getProviderPartialTotals(providerName));
        }

        Request mapPayload = Request.providerMapPayload(
                providerName,
                request.getRequestId(),
                storage.getProviderPartialTotals(providerName)
        );
        return sendMapResultToReducer(request, mapPayload);


    }

    private Response handleMapPlayerStats(Request request) {
        String playerId = request.getPlayerId();
        if (playerId == null || playerId.trim().isEmpty()) {
            return new Response(false, "Player ID is empty");
        }

        if (request.getRequestId() == null || request.getRequestId().isBlank()) {
            return new Response(true, "Player map output ready", storage.getPlayerPartialTotals(playerId));
        }

        Request mapPayload = Request.playerMapPayload(
                playerId,
                request.getRequestId(),
                storage.getPlayerPartialTotals(playerId)
        );
        return sendMapResultToReducer(request, mapPayload);
    }

    private Response sendMapResultToReducer(Request mapJob, Request mapPayload) {
        String reducerHost = mapJob.getReducerHost() == null || mapJob.getReducerHost().isBlank()
                ? "localhost"
                : mapJob.getReducerHost();
        int reducerPort = mapJob.getReducerPort() == null ? 7000 : mapJob.getReducerPort();

        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            socket = new Socket(reducerHost, reducerPort);

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            in = new ObjectInputStream(socket.getInputStream());

            out.writeObject(mapPayload);
            out.flush();

            Response response = (Response) in.readObject();
            if (!response.isSuccess()) {
                return response;
            }

            return new Response(true, "Map output submitted to reducer");
        } catch (Exception e) {
            return new Response(false, "Failed to submit map output to reducer: " + e.getMessage());
        } finally {
            try {
                if (in != null) in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (out != null) out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private GameInfo convertToGameInfo(Game game) {
        return new GameInfo(
                game.getGameName(),
                game.getProviderName(),
                game.getStars(),
                game.getNoOfVotes(),
                game.getGameLogo(),
                game.getMinBet(),
                game.getMaxBet(),
                game.getRiskLevel(),
                game.getHashKey()
        );
    }
}
