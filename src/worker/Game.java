package worker;

import common.GameInfo;

import java.io.Serializable;

public class Game implements Serializable {

    private String gameName;
    private String providerName;
    private int stars;
    private int noOfVotes;
    private String gameLogo;
    private double minBet;
    private double maxBet;
    private String riskLevel;
    private String hashKey;
    private boolean active;
    private double jackpot;
    private double totalBetAmount;
    private double totalPayoutAmount;

    @Override
    public String toString() {
        return "Game{" +
                "name='" + gameName + '\'' +
                ", risk='" + riskLevel + '\'' +
                ", active=" + active +
                '}';
    }

    public Game(GameInfo info) {
        this.gameName = info.getGameName();
        this.providerName = info.getProviderName();
        this.stars = info.getStars();
        this.noOfVotes = info.getNoOfVotes();
        this.gameLogo = info.getGameLogo();
        this.minBet = info.getMinBet();
        this.maxBet = info.getMaxBet();
        this.riskLevel = info.getRiskLevel();
        this.hashKey = info.getHashKey();
        this.active = true;
        this.jackpot = calculateJackpot(info.getRiskLevel());
        this.totalBetAmount = 0;
        this.totalPayoutAmount = 0;
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

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    private double calculateJackpot(String riskLevel) {
        if (riskLevel == null) return 10;

        if (riskLevel.equalsIgnoreCase("low")) return 10;
        if (riskLevel.equalsIgnoreCase("medium")) return 20;
        if (riskLevel.equalsIgnoreCase("high")) return 40;

        return 10;
    }

    public double getJackpot() {
        return jackpot;
    }

    public double getTotalBetAmount() {
        return totalBetAmount;
    }

    public double getTotalPayoutAmount() {
        return totalPayoutAmount;
    }

    public void addToTotalBetAmount(double amount) {
        totalBetAmount += amount;
    }

    public void addToTotalPayoutAmount(double amount) {
        totalPayoutAmount += amount;
    }

    public void setMinBet(double minBet) {
        this.minBet = minBet;
    }

    public void setMaxBet(double maxBet) {
        this.maxBet = maxBet;
    }

    public void setRiskLevel(String riskLevel) {
        this.riskLevel = riskLevel;
        this.jackpot = calculateJackpot(riskLevel);
    }

    public void addRating(int rating) {
        int updatedVotes = noOfVotes + 1;
        stars = Math.round((stars * noOfVotes + rating) / (float) updatedVotes);
        noOfVotes = updatedVotes;
    }
}
