package master;

import common.Request;
import common.Response;
import common.WorkerInfo;

import java.util.List;
import java.util.UUID;

public class MasterDispatcher {
    private final HashRouter hashRouter;
    private final WorkerRegistry workerRegistry;
    private final WorkerClient workerClient;
    private final ReducerClient reducerClient;
    private final CasinoState casinoState;

    public MasterDispatcher(HashRouter hashRouter,
                            WorkerRegistry workerRegistry,
                            WorkerClient workerClient,
                            ReducerClient reducerClient) {
        this.hashRouter = hashRouter;
        this.workerRegistry = workerRegistry;
        this.workerClient = workerClient;
        this.reducerClient = reducerClient;
        this.casinoState = new CasinoState();
    }

    public Response dispatch(Request request) {
        if (request == null || request.getType() == null) {
            return new Response(false, "Invalid request");
        }

        return switch (request.getType()) {
            case ADD_GAME -> addGameToMasterAndWorker(request);
            case REMOVE_GAME -> removeGameFromMasterAndWorker(request);
            case UPDATE_GAME_RISK -> updateRiskOnMasterAndWorker(request);
            case UPDATE_GAME_BET_LIMITS -> updateBetLimitsOnMasterAndWorker(request);
            case GET_PROVIDER_STATS -> reduceProviderFromWorkers(request.getProviderName());
            case GET_PLAYER_STATS -> reducePlayerFromWorkers(request.getPlayerId());
            case GET_ALL_GAMES -> casinoState.getAllAvailableGames();
            case SEARCH_GAMES ->
                    casinoState.search(request.getProviderName(), request.getRiskLevel(), request.getBetCategory(), request.getMinStars());
            case PLACE_BET -> routeByGameName(request.getGameName(), request);
            case ADD_BALANCE -> broadcastToWorkers(request);
            case RATE_GAME -> rateGameOnMasterAndWorker(request);
            default -> new Response(false, "Unsupported request type for master: " + request.getType());
        };
    }

    private Response addGameToMasterAndWorker(Request request) {
        Response masterResponse = casinoState.addGame(request.getGameInfo());
        if (!masterResponse.isSuccess()) {
            return masterResponse;
        }

        Response workerResponse = routeByGameName(request.getGameInfo().getGameName(), request);
        if (!workerResponse.isSuccess()) {
            casinoState.removeGame(request.getGameInfo().getGameName());
        }
        return workerResponse;
    }

    private Response removeGameFromMasterAndWorker(Request request) {
        Response masterResponse = casinoState.removeGame(request.getGameName());
        if (!masterResponse.isSuccess()) {
            return masterResponse;
        }

        return routeByGameName(request.getGameName(), request);
    }

    private Response updateRiskOnMasterAndWorker(Request request) {
        Response masterResponse = casinoState.updateRisk(request.getGameName(), request.getRiskLevel());
        if (!masterResponse.isSuccess()) {
            return masterResponse;
        }
        return routeByGameName(request.getGameName(), request);
    }

    private Response updateBetLimitsOnMasterAndWorker(Request request) {
        Response masterResponse = casinoState.updateBetLimits(request.getGameName(), request.getMinBet(), request.getMaxBet());
        if (!masterResponse.isSuccess()) {
            return masterResponse;
        }
        return routeByGameName(request.getGameName(), request);
    }

    private Response rateGameOnMasterAndWorker(Request request) {
        if (request.getMinStars() == null) {
            return new Response(false, "Rating is missing");
        }

        Response masterResponse = casinoState.rateGame(request.getGameName(), request.getMinStars());
        if (!masterResponse.isSuccess()) {
            return masterResponse;
        }

        Response workerResponse = routeByGameName(request.getGameName(), request);
        if (!workerResponse.isSuccess()) {
            return workerResponse;
        }

        return masterResponse;
    }

    private Response routeByGameName(String gameName, Request request) {
        try {
            WorkerInfo targetWorker = hashRouter.routeByGameName(gameName);
            return workerClient.sendRequest(targetWorker, request);
        } catch (Exception e) {
            return new Response(false, e.getMessage());
        }
    }

    private Response reduceProviderFromWorkers(String providerName) {
        if (providerName == null || providerName.isBlank()) {
            return new Response(false, "Provider name is empty");
        }
        String requestId = UUID.randomUUID().toString();
        Response startResponse = reducerClient.startReduce(Request.startProviderReduce(providerName, requestId, workerRegistry.size()));
        if (!startResponse.isSuccess()) {
            return startResponse;
        }

        Response mapResponse = broadcastMapJobs(Request.providerMapJob(
                providerName, requestId, reducerClient.getReducerHost(), reducerClient.getReducerPort()));
        if (!mapResponse.isSuccess()) {
            return mapResponse;
        }

        return reducerClient.waitForResult(requestId);
    }

    private Response reducePlayerFromWorkers(String playerId) {
        if (playerId == null || playerId.isBlank()) {
            return new Response(false, "Player ID is empty");
        }
        String requestId = UUID.randomUUID().toString();
        Response startResponse = reducerClient.startReduce(Request.startPlayerReduce(playerId, requestId, workerRegistry.size()));
        if (!startResponse.isSuccess()) {
            return startResponse;
        }

        Response mapResponse = broadcastMapJobs(Request.playerMapJob(
                playerId, requestId, reducerClient.getReducerHost(), reducerClient.getReducerPort()));
        if (!mapResponse.isSuccess()) {
            return mapResponse;
        }

        return reducerClient.waitForResult(requestId);
    }

    private Response broadcastMapJobs(Request request) {
        List<Response> responses = workerClient.broadcast(workerRegistry.getWorkers(), request);
        for (Response response : responses) {
            if (!response.isSuccess()) {
                return new Response(false, "Worker map phase failed: " + response.getMessage());
            }
        }
        return new Response(true, "Workers submitted map outputs to reducer");
    }

    private Response broadcastToWorkers(Request request) {
        List<Response> responses = workerClient.broadcast(workerRegistry.getWorkers(), request);
        for (Response response : responses) {
            if (!response.isSuccess()) {
                return new Response(false, "Worker request failed: " + response.getMessage());
            }
        }
        return new Response(true, "Request applied to all workers");
    }

}
