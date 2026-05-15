package common;

import java.io.Serializable;

public class GameInfo implements Serializable {
    private final String gameName;
    private final String providerName;
    private final int stars;
    private final int noOfVotes;
    private final String gameLogo;
    private final double minBet;
    private final double maxBet;
    private final String riskLevel;
    private final String hashKey;
    private final String betCategory;
    private final double jackpotMultiplier;

    public GameInfo(String gameName,
                    String providerName,
                    int stars,
                    int noOfVotes,
                    String gameLogo,
                    double minBet,
                    double maxBet,
                    String riskLevel,
                    String hashKey) {
        this.gameName = gameName;
        this.providerName = providerName;
        this.stars = stars;
        this.noOfVotes = noOfVotes;
        this.gameLogo = gameLogo;
        this.minBet = minBet;
        this.maxBet = maxBet;
        this.riskLevel = riskLevel;
        this.hashKey = hashKey;
        this.betCategory = deriveBetCategory(minBet);
        this.jackpotMultiplier = deriveJackpotMultiplier(riskLevel);
    }

    public String getGameName() {
        return gameName;
    }

    public String getProviderName() {
        return providerName;
    }

    public int getStars() {
        return stars;
    }

    public int getNoOfVotes() {
        return noOfVotes;
    }

    public String getGameLogo() {
        return gameLogo;
    }

    public double getMinBet() {
        return minBet;
    }

    public double getMaxBet() {
        return maxBet;
    }

    public String getRiskLevel() {
        return riskLevel;
    }

    public String getHashKey() {
        return hashKey;
    }

    public String getBetCategory() {
        return betCategory;
    }

    public double getJackpotMultiplier() {
        return jackpotMultiplier;
    }

    public GameInfo withRiskLevel(String updatedRiskLevel) {
        return new GameInfo(gameName, providerName, stars, noOfVotes, gameLogo, minBet, maxBet, updatedRiskLevel, hashKey);
    }

    public GameInfo withRating(int rating) {
        int updatedVotes = noOfVotes + 1;
        int updatedStars = Math.round((stars * noOfVotes + rating) / (float) updatedVotes);
        return new GameInfo(gameName, providerName, updatedStars, updatedVotes, gameLogo, minBet, maxBet, riskLevel, hashKey);
    }

    private static String deriveBetCategory(double minBet) {
        if (minBet >= 5.0) {
            return "$$$";
        }
        if (minBet >= 1.0) {
            return "$$";
        }
        return "$";
    }

    private static double deriveJackpotMultiplier(String riskLevel) {
        if (riskLevel == null) {
            return 10.0;
        }

        return switch (riskLevel.trim().toLowerCase()) {
            case "medium" -> 20.0;
            case "high" -> 40.0;
            default -> 10.0;
        };
    }
}
