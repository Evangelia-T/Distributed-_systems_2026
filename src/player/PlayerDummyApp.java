package player;

import common.GameInfo;
import common.Request;
import common.Response;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;

public class PlayerDummyApp {
    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;

        try (Scanner scanner = new Scanner(System.in)) {
            boolean running = true;
            while (running) {
                printMenu();
                String option = scanner.nextLine().trim();
                switch (option) {
                    case "1" -> listAllAvailableGames(host, port);
                    case "2" -> searchGames(host, port, scanner);
                    case "3" -> addBalance(host, port, scanner);
                    case "4" -> play(host, port, scanner);
                    case "5" -> playerStats(host, port, scanner);
                    case "0" -> running = false;
                    default -> System.out.println("Unknown option");
                }
            }
        } catch (Exception e) {
            System.err.println("Player dummy app failed: " + e.getMessage());
        }
    }

    private static void listAllAvailableGames(String host, int port) throws Exception {
        Response response = send(host, port, Request.getAllGames());
        System.out.println((response.isSuccess() ? "OK" : "ERROR") + ": " + response.getMessage());

        if (response.getGames() == null || response.getGames().isEmpty()) {
            System.out.println("No available games found.");
            return;
        }

        for (GameInfo game : response.getGames()) {
            System.out.printf("- %s | provider=%s | stars=%d | risk=%s | bet=%s | range=[%.2f..%.2f]%n",
                    game.getGameName(), game.getProviderName(), game.getStars(), game.getRiskLevel(), game.getBetCategory(), game.getMinBet(), game.getMaxBet());
        }
    }

    private static void searchGames(String host, int port, Scanner scanner) throws Exception {
        System.out.print("Player ID: ");
        String playerId = scanner.nextLine().trim();
        System.out.print("Provider filter (blank = any): ");
        String provider = scanner.nextLine().trim();
        System.out.print("Risk filter low/medium/high (blank = any): ");
        String risk = scanner.nextLine().trim();
        System.out.print("Bet category $, $$, $$$ (blank = any): ");
        String category = scanner.nextLine().trim();
        System.out.print("Minimum stars 1-5 (blank = any): ");
        String minStarsRaw = scanner.nextLine().trim();

        Integer minStars = minStarsRaw.isBlank() ? null : Integer.parseInt(minStarsRaw);
        Response response = send(host, port, Request.searchGames(playerId, emptyToNull(provider), emptyToNull(risk), emptyToNull(category), minStars));

        System.out.println((response.isSuccess() ? "OK" : "ERROR") + ": " + response.getMessage());
        for (GameInfo game : response.getGames()) {
            System.out.printf("- %s | provider=%s | stars=%d | risk=%s | bet=%s | range=[%.2f..%.2f]%n",
                    game.getGameName(), game.getProviderName(), game.getStars(), game.getRiskLevel(), game.getBetCategory(), game.getMinBet(), game.getMaxBet());
        }
    }

    private static void addBalance(String host, int port, Scanner scanner) throws Exception {
        System.out.print("Player ID: ");
        String playerId = scanner.nextLine().trim();
        System.out.print("Amount to add: ");
        double amount = Double.parseDouble(scanner.nextLine().trim());
        Response response = send(host, port, Request.addBalance(playerId, amount));
        System.out.println((response.isSuccess() ? "OK" : "ERROR") + ": " + response.getMessage());
    }

    private static void play(String host, int port, Scanner scanner) throws Exception {
        System.out.print("Player ID: ");
        String playerId = scanner.nextLine().trim();
        System.out.print("Game name: ");
        String gameName = scanner.nextLine().trim();
        System.out.print("Bet amount: ");
        double amount = Double.parseDouble(scanner.nextLine().trim());

        Response response = send(host, port, Request.placeBet(playerId, gameName, amount));
        System.out.println((response.isSuccess() ? "OK" : "ERROR") + ": " + response.getMessage());
    }

    private static void playerStats(String host, int port, Scanner scanner) throws Exception {
        System.out.print("Player ID: ");
        String playerId = scanner.nextLine().trim();
        Response response = send(host, port, Request.playerStats(playerId));
        System.out.println((response.isSuccess() ? "OK" : "ERROR") + ": " + response.getMessage());
        response.getTotals().forEach((k, v) -> System.out.printf("  %s -> %.2f FUN%n", k, v));
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

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static void printMenu() {
        System.out.println();
        System.out.println("=== Player Dummy App ===");
        System.out.println("1. List all available games");
        System.out.println("2. search() with filters");
        System.out.println("3. addBalance()");
        System.out.println("4. play()");
        System.out.println("5. View my stats");
        System.out.println("0. Exit");
        System.out.print("Choose option: ");
    }
}
