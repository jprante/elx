package org.xbib.elx.http.action.admin.cluster.health;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

public class HttpClusterHealthResponse extends ClusterHealthResponse {

    private String clusterName;

    private ClusterStateHealth clusterStateHealth;

    private boolean timedOut;

    private int delayedUnassignedShards;

    private int numberOfPendingTasks;

    private int numberOfInFlightFetch;

    public HttpClusterHealthResponse() {
    }

    private void init(String clusterName,
                      ClusterHealthStatus clusterHealthStatus,
                      boolean timedOut,
                      int numberOfNodes,
                      int numberOfDataNodes,
                      Map<String, ClusterIndexHealth> indices,
                      int activePrimaryShards,
                      int activeShards,
                      int relocatingShards,
                      int initializingShards,
                      int unassignedShards,
                      int delayedUnassignedShards,
                      int numberOfPendingTasks, int numberOfInFlightFetch,
                      TimeValue taskMaxWaitingTime,
                      double activeShardsPercent) throws IOException {
        this.clusterName = clusterName;
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.writeVInt(activePrimaryShards);
        streamOutput.writeVInt(activeShards);
        streamOutput.writeVInt(relocatingShards);
        streamOutput.writeVInt(initializingShards);
        streamOutput.writeVInt(unassignedShards);
        streamOutput.writeVInt(numberOfNodes);
        streamOutput.writeVInt(numberOfDataNodes);
        streamOutput.writeByte(clusterHealthStatus.value());
        streamOutput.writeVInt(indices.size());
        for (ClusterIndexHealth indexHealth : indices.values()) {
            indexHealth.writeTo(streamOutput);
        }
        streamOutput.writeDouble(activeShardsPercent);
        this.clusterStateHealth = new ClusterStateHealth(streamOutput.bytes().streamInput());
        this.timedOut = timedOut;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public boolean isTimedOut() {
        return this.timedOut;
    }

    @Override
    public int getActiveShards() {
        return clusterStateHealth.getActiveShards();
    }

    @Override
    public int getRelocatingShards() {
        return clusterStateHealth.getRelocatingShards();
    }

    @Override
    public int getActivePrimaryShards() {
        return clusterStateHealth.getActivePrimaryShards();
    }

    @Override
    public int getInitializingShards() {
        return clusterStateHealth.getInitializingShards();
    }

    @Override
    public int getUnassignedShards() {
        return clusterStateHealth.getUnassignedShards();
    }

    @Override
    public int getDelayedUnassignedShards() {
        return delayedUnassignedShards;
    }

    @Override
    public int getNumberOfNodes() {
        return clusterStateHealth.getNumberOfNodes();
    }

    @Override
    public int getNumberOfDataNodes() {
        return clusterStateHealth.getNumberOfDataNodes();
    }

    @Override
    public int getNumberOfPendingTasks() {
        return numberOfPendingTasks;
    }

    @Override
    public int getNumberOfInFlightFetch() {
        return numberOfInFlightFetch;
    }

    @Override
    public ClusterHealthStatus getStatus() {
        return clusterStateHealth.getStatus();
    }

    @Override
    public Map<String, ClusterIndexHealth> getIndices() {
        return clusterStateHealth.getIndices();
    }

    @Override
    public double getActiveShardsPercent() {
        return clusterStateHealth.getActiveShardsPercent();
    }

    @Override
    public RestStatus status() {
        return isTimedOut() ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
    }
}