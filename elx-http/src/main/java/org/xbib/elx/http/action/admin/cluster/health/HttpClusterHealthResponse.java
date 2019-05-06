package org.xbib.elx.http.action.admin.cluster.health;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class HttpClusterHealthResponse extends ClusterHealthResponse {

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String STATUS = "status";
    private static final String TIMED_OUT = "timed_out";
    private static final String NUMBER_OF_NODES = "number_of_nodes";
    private static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    private static final String NUMBER_OF_PENDING_TASKS = "number_of_pending_tasks";
    private static final String NUMBER_OF_IN_FLIGHT_FETCH = "number_of_in_flight_fetch";
    private static final String DELAYED_UNASSIGNED_SHARDS = "delayed_unassigned_shards";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE = "task_max_waiting_in_queue";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS = "task_max_waiting_in_queue_millis";
    private static final String ACTIVE_SHARDS_PERCENT_AS_NUMBER = "active_shards_percent_as_number";
    private static final String ACTIVE_SHARDS_PERCENT = "active_shards_percent";
    private static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String INDICES = "indices";

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
    public ClusterStateHealth getClusterStateHealth() {
        return clusterStateHealth;
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

    public static HttpClusterHealthResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        Map<String, Object> map = parser.map();
        String clusterName = (String) map.get(CLUSTER_NAME);
        ClusterHealthStatus status = ClusterHealthStatus.fromString((String) map.get(STATUS));
        Boolean timedOut = (Boolean) map.get(TIMED_OUT);
        Integer numberOfNodes = (Integer) map.get(NUMBER_OF_NODES);
        Integer numberOfDataNodes = (Integer) map.get(NUMBER_OF_DATA_NODES);
        Integer activePrimaryShards = (Integer) map.get(ACTIVE_PRIMARY_SHARDS);
        Integer activeShards = (Integer) map.get(ACTIVE_SHARDS);
        Integer relocatingShards = (Integer) map.get(RELOCATING_SHARDS);
        Integer initializingShards = (Integer) map.get(INITIALIZING_SHARDS);
        Integer unassignedShards = (Integer) map.get(UNASSIGNED_SHARDS);
        Integer delayedUnassignedShards = (Integer) map.get(DELAYED_UNASSIGNED_SHARDS);
        Integer numberOfPendingTasks = (Integer) map.get(NUMBER_OF_PENDING_TASKS);
        Integer numberOfInFlightFetch = (Integer) map.get(NUMBER_OF_IN_FLIGHT_FETCH);
        Integer taskMaxWaitingInQueueMillis = (Integer) map.get(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS);
        Double activeShardsPercentAsNumber = (Double) map.get(ACTIVE_SHARDS_PERCENT_AS_NUMBER);
        HttpClusterHealthResponse clusterHealthResponse = new HttpClusterHealthResponse();
        clusterHealthResponse.init(clusterName,
                status,
                timedOut,
                numberOfNodes,
                numberOfDataNodes,
                Collections.emptyMap(),
                activePrimaryShards,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                delayedUnassignedShards,
                numberOfPendingTasks,
                numberOfInFlightFetch,
                TimeValue.timeValueMillis(taskMaxWaitingInQueueMillis),
                activeShardsPercentAsNumber
        );
        return clusterHealthResponse;
    }
}