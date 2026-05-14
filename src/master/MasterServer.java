package master;

import common.WorkerInfo;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MasterServer {
    private static final int DEFAULT_MASTER_PORT = 5000;
    private static final String DEFAULT_REDUCER_HOST = "localhost";
    private static final int DEFAULT_REDUCER_PORT = 7000;

    public static void main(String[] args) throws Exception {
        int masterPort = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_MASTER_PORT;
        String reducerHost = args.length > 1 ? args[1] : DEFAULT_REDUCER_HOST;
        int reducerPort = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_REDUCER_PORT;

        List<WorkerInfo> workers = parseWorkers(args, 3);
        WorkerRegistry workerRegistry = new WorkerRegistry(workers);
        HashRouter hashRouter = new HashRouter(workerRegistry);
        WorkerClient workerClient = new WorkerClient();
        ReducerClient reducerClient = new ReducerClient(reducerHost, reducerPort);
        MasterDispatcher dispatcher = new MasterDispatcher(hashRouter, workerRegistry, workerClient, reducerClient);

        try (ServerSocket serverSocket = new ServerSocket(masterPort)) {
            System.out.println("Master started on port " + masterPort);
            System.out.println("Reducer configured at " + reducerHost + ":" + reducerPort);
            System.out.println("Workers: " + workerRegistry.getWorkers().size());

            while (true) {
                Socket clientSocket = serverSocket.accept();
                Thread thread = new Thread(new MasterClientHandler(clientSocket, dispatcher));
                thread.start();
            }
        }
    }

    private static List<WorkerInfo> parseWorkers(String[] args, int startIndex) {
        List<WorkerInfo> workers = new ArrayList<>();
        for (int i = startIndex; i < args.length; i++) {
            String[] parts = args[i].split(":");
            if (parts.length == 2) {
                workers.add(new WorkerInfo(parts[0], Integer.parseInt(parts[1])));
            }
        }

        if (workers.isEmpty()) {
            workers.add(new WorkerInfo("localhost", 8081));
        }
        return workers;
    }
}
