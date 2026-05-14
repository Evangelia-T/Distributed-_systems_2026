package manager;

import common.GameInfo;
import common.Request;
import common.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Scanner;

public class ManagerConsoleApp {
    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;

        try (Scanner scanner = new Scanner(System.in)) {
            boolean running = true;
            while (running) {
                printMenu();
                String option = scanner.nextLine().trim();
                switch (option) {
                    case "1" -> printResponse(send(host, port, buildAddGameRequest(scanner)));
                    case "2" -> {
                        System.out.print("Game name to remove: ");
                        printResponse(send(host, port, Request.removeGame(scanner.nextLine().trim())));
                    }
                    case "3" -> {
                        System.out.print("Game name to update: ");
                        String gameName = scanner.nextLine().trim();
                        System.out.print("New risk level (low/medium/high): ");
                        printResponse(send(host, port, Request.updateGameRisk(gameName, scanner.nextLine().trim())));
                    }
                    case "4" -> {
                        System.out.print("Game name to update bet limits: ");
                        String gameName = scanner.nextLine().trim();
                        System.out.print("New min bet: ");
                        double minBet = Double.parseDouble(scanner.nextLine().trim());
                        System.out.print("New max bet: ");
                        double maxBet = Double.parseDouble(scanner.nextLine().trim());
                        printResponse(send(host, port, Request.updateGameBetLimits(gameName, minBet, maxBet)));
                    }
                    case "5" -> {
                        System.out.print("Provider name: ");
                        printResponse(send(host, port, Request.providerStats(scanner.nextLine().trim())));
                    }
                    case "6" -> {
                        System.out.print("Player id: ");
                        printResponse(send(host, port, Request.playerStats(scanner.nextLine().trim())));
                    }
                    case "0" -> running = false;
                    default -> System.out.println("Unknown option. Please try again.");
                }
            }
        } catch (Exception e) {
            System.err.println("Manager console failed: " + e.getMessage());
        }
    }

    private static Request buildAddGameRequest(Scanner scanner) {
        System.out.print("Load game from JSON file.");
        GameInfo gameInfo = buildGameFromJson(scanner);
        return Request.addGame(gameInfo);
    }

    private static GameInfo buildGameFromJson(Scanner scanner) {
        try {
            System.out.print("JSON file path: ");
            String jsonPath = scanner.nextLine().trim();
            return GameJsonLoader.loadFromPath(jsonPath);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to load game JSON: " + e.getMessage(), e);
        }
    }

    private static Response send(String host, int port, Request request) throws Exception {
        try (Socket socket = new Socket(host, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(request);
            out.flush();
            return (Response) in.readObject();
        }
    }

    private static void printResponse(Response response) {
        System.out.println((response.isSuccess() ? "OK" : "ERROR") + ": " + response.getMessage());
        for (Map.Entry<String, Double> entry : response.getTotals().entrySet()) {
            System.out.printf("  %s -> %.2f FUN%n", entry.getKey(), entry.getValue());
        }
    }

    private static void printMenu() {
        System.out.println();
        System.out.println("=== Manager Console ===");
        System.out.println("1. Add game");
        System.out.println("2. Remove game");
        System.out.println("3. Update game risk");
        System.out.println("4. Update game bet limits");
        System.out.println("5. Provider profit/loss report");
        System.out.println("6. Player profit/loss report");
        System.out.println("0. Exit");
        System.out.print("Choose option: ");
    }
}
