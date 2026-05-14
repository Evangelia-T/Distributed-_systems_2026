package common;

import java.io.Serializable;

public class WorkerInfo implements Serializable {
    private final String host;
    private final int port;

    public WorkerInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
}