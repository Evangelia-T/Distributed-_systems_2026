package rng;

import common.GameInfo;
import common.Request;
import common.RequestType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

public class RandomRequestHandler extends Thread {

    private static final int QUEUE_REFILL_SIZE = 100;
    private static final Map<String, GameRandomQueue> GAME_QUEUES = new HashMap<>();

    private final Socket client;

    public RandomRequestHandler(Socket client) {
        this.client = client;
    }

    @Override
    public void run() {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        try {
            out = new ObjectOutputStream(client.getOutputStream());
            out.flush();

            in = new ObjectInputStream(client.getInputStream());

            Object payload = in.readObject();
            Object response = handlePayload(payload);

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

    private Object handlePayload(Object payload) {
        if (payload instanceof Request request) {
            return handleRequest(request);
        }

        if (payload instanceof String secret) {
            int randomNumber = new Random().nextInt(1000);
            return new RandomResult(randomNumber, sha256(randomNumber + secret));
        }

        return new RandomResult(-1, "Unsupported RNG request payload");
    }

    private RandomResult handleRequest(Request request) {
        if (request == null || request.getType() == null) {
            return new RandomResult(-1, "Invalid RNG request");
        }

        RequestType type = request.getType();
        return switch (type) {
            case ADD_GAME -> registerGame(request.getGameInfo());
            case REMOVE_GAME -> removeGame(request.getGameName());
            case GET_RANDOM_NUMBER -> nextRandomForGame(request.getGameName());
            default -> new RandomResult(-1, "Unsupported RNG request type: " + type);
        };
    }

    private RandomResult registerGame(GameInfo gameInfo) {
        if (gameInfo == null || isBlank(gameInfo.getGameName()) || isBlank(gameInfo.getHashKey())) {
            return new RandomResult(-1, "Missing game name or secret");
        }

        GameRandomQueue oldQueue = putGameQueue(gameInfo.getGameName(), new GameRandomQueue(gameInfo.getHashKey()));
        if (oldQueue != null) {
            oldQueue.close();
        }
        return new RandomResult(0, "RNG queue registered for game: " + gameInfo.getGameName());
    }

    private RandomResult removeGame(String gameName) {
        if (isBlank(gameName)) {
            return new RandomResult(-1, "Game name is empty");
        }

        GameRandomQueue removedQueue = removeGameQueue(gameName);
        if (removedQueue != null) {
            removedQueue.close();
        }
        return new RandomResult(0, "RNG queue removed for game: " + gameName);
    }

    private RandomResult nextRandomForGame(String gameName) {
        if (isBlank(gameName)) {
            return new RandomResult(-1, "Game name is empty");
        }

        GameRandomQueue queue = getGameQueue(gameName);
        if (queue == null) {
            return new RandomResult(-1, "No RNG queue registered for game: " + gameName);
        }

        try {
            int randomNumber = queue.nextRandomNumber();
            return new RandomResult(randomNumber, sha256(randomNumber + queue.getSecret()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new RandomResult(-1, "Interrupted while waiting for random number");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static synchronized GameRandomQueue putGameQueue(String gameName, GameRandomQueue queue) {
        return GAME_QUEUES.put(gameName, queue);
    }

    private static synchronized GameRandomQueue removeGameQueue(String gameName) {
        return GAME_QUEUES.remove(gameName);
    }

    private static synchronized GameRandomQueue getGameQueue(String gameName) {
        return GAME_QUEUES.get(gameName);
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

    private static class GameRandomQueue {
        private final String secret;
        private final Random random = new Random();
        private final Queue<Integer> randomNumbers = new ArrayDeque<>();
        private boolean active = true;

        private GameRandomQueue(String secret) {
            this.secret = secret;
            Thread producer = new Thread(this::produceRandomNumbers, "rng-producer-" + Math.abs(secret.hashCode()));
            producer.setDaemon(true);
            producer.start();
        }

        private String getSecret() {
            return secret;
        }

        private synchronized int nextRandomNumber() throws InterruptedException {
            while (active && randomNumbers.isEmpty()) {
                wait();
            }
            if (!active) {
                throw new InterruptedException("RNG queue is closed");
            }

            int randomNumber = randomNumbers.remove();
            notifyAll();
            return randomNumber;
        }

        private void close() {
            synchronized (this) {
                active = false;
                notifyAll();
            }
        }

        private void produceRandomNumbers() {
            while (true) {
                synchronized (this) {
                    while (active && randomNumbers.size() >= QUEUE_REFILL_SIZE) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    if (!active) {
                        return;
                    }

                    randomNumbers.add(random.nextInt(1000));
                    notifyAll();
                }
            }
        }
    }
}
