package master;

import common.WorkerInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WorkerRegistry {
    private final List<WorkerInfo> workers = new ArrayList<>();

    public WorkerRegistry(List<WorkerInfo> workers) {
        if (workers == null || workers.isEmpty()) {
            throw new IllegalArgumentException("At least one worker is required");
        }
        this.workers.addAll(workers);
    }

    public List<WorkerInfo> getWorkers() {
        return Collections.unmodifiableList(workers);
    }

    public int size() {
        return workers.size();
    }

    public WorkerInfo getByIndex(int index) {
        return workers.get(index);
    }
}
