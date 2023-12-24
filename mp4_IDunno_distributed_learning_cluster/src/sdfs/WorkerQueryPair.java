package sdfs;

import ml.MLQueryData;

/**
 * Data class to return with two values
 */
public class WorkerQueryPair {
    public String workerIp;
    public MLQueryData mlQueryData;

    public WorkerQueryPair(String workerIp, MLQueryData mlQueryData) {
        this.workerIp = workerIp;
        this.mlQueryData = mlQueryData;
    }

    @Override
    public String toString() {
        if (mlQueryData != null) {
            return workerIp + ":" + mlQueryData.getContentList();
        } else {
            return workerIp + ":" + mlQueryData;
        }
    }
}
