package rng;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class RandomRequestHandler extends Thread {

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

            String secret = (String) in.readObject();

            int randomNumber = new Random().nextInt(1000);
            String hash = sha256(randomNumber + secret);

            RandomResult result = new RandomResult(randomNumber, hash);

            out.writeObject(result);
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
}