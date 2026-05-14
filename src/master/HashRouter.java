package master;

import common.WorkerInfo;

public class HashRouter {
    private final WorkerRegistry workerRegistry;

    public HashRouter(WorkerRegistry workerRegistry) {
        this.workerRegistry = workerRegistry;
    }

    public WorkerInfo routeByGameName(String gameName) {
        if (gameName == null || gameName.isBlank()) {
            throw new IllegalArgumentException("gameName is required for routing");
        }

        int hash = gameName.hashCode() & Integer.MAX_VALUE;
        int index = hash % workerRegistry.size();
        return workerRegistry.getByIndex(index);
    }
}
