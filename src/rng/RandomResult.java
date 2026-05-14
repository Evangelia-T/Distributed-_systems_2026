package rng;

import java.io.Serializable;

public class RandomResult implements Serializable {
    private final int randomNumber;
    private final String hash;

    public RandomResult(int randomNumber, String hash) {
        this.randomNumber = randomNumber;
        this.hash = hash;
    }

    public int getRandomNumber() {
        return randomNumber;
    }

    public String getHash() {
        return hash;
    }
}