package org.xbib.elx.http.action.admin.cluster.health;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class HttpClusterHealthAction extends HttpAction<ClusterHealthRequest, ClusterHealthResponse> {

    @Override
    public ClusterHealthAction getActionInstance() {
        return ClusterHealthAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, ClusterHealthRequest request) {
        return newPutRequest(url, "/_cluster/health");
    }

    @Override
    protected CheckedFunction<XContentParser, ClusterHealthResponse, IOException> entityParser() {
        throw new UnsupportedOperationException();
    }

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

    private static final ConstructingObjectParser<ClusterHealthResponse, Void> PARSER =
            new ConstructingObjectParser<>("cluster_health_response", true,
                    parsedObjects -> {
                        int i = 0;
                        // ClusterStateHealth fields
                        int numberOfNodes = (int) parsedObjects[i++];
                        int numberOfDataNodes = (int) parsedObjects[i++];
                        int activeShards = (int) parsedObjects[i++];
                        int relocatingShards = (int) parsedObjects[i++];
                        int activePrimaryShards = (int) parsedObjects[i++];
                        int initializingShards = (int) parsedObjects[i++];
                        int unassignedShards = (int) parsedObjects[i++];
                        double activeShardsPercent = (double) parsedObjects[i++];
                        String statusStr = (String) parsedObjects[i++];
                        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
                        @SuppressWarnings("unchecked") List<ClusterIndexHealth> indexList =
                                (List<ClusterIndexHealth>) parsedObjects[i++];
                        final Map<String, ClusterIndexHealth> indices;
                        if (indexList == null || indexList.isEmpty()) {
                            indices = emptyMap();
                        } else {
                            indices = new HashMap<>(indexList.size());
                            for (ClusterIndexHealth indexHealth : indexList) {
                                indices.put(indexHealth.getIndex(), indexHealth);
                            }
                        }
                        /*ClusterStateHealth stateHealth = new ClusterStateHealth(activePrimaryShards, activeShards, relocatingShards,
                                initializingShards, unassignedShards, numberOfNodes, numberOfDataNodes, activeShardsPercent, status,
                                indices);*/
                        //ClusterState clusterState = new ClusterState();
                        //ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);

                        // ClusterHealthResponse fields
                        String clusterName = (String) parsedObjects[i++];
                        int numberOfPendingTasks = (int) parsedObjects[i++];
                        int numberOfInFlightFetch = (int) parsedObjects[i++];
                        int delayedUnassignedShards = (int) parsedObjects[i++];
                        long taskMaxWaitingTimeMillis = (long) parsedObjects[i++];
                        boolean timedOut = (boolean) parsedObjects[i];

                        return new ClusterHealthResponse(clusterName, null, null, numberOfPendingTasks,
                                numberOfInFlightFetch, delayedUnassignedShards,
                                TimeValue.timeValueMillis(taskMaxWaitingTimeMillis));
                        /*return new ClusterHealthResponse(clusterName, numberOfPendingTasks, numberOfInFlightFetch,
                                 delayedUnassignedShards,
                                TimeValue.timeValueMillis(taskMaxWaitingTimeMillis), timedOut, stateHealth);*/
                    });


   // private static final ObjectParser.NamedObjectParser<ClusterIndexHealth, Void> INDEX_PARSER =
   //         (XContentParser parser, Void context, String index) -> ClusterIndexHealth.innerFromXContent(parser, index);

    static {
        // ClusterStateHealth fields
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_NODES));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_DATA_NODES));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_PRIMARY_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareDouble(constructorArg(), new ParseField(ACTIVE_SHARDS_PERCENT_AS_NUMBER));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
        // Can be absent if LEVEL == 'cluster'
        //PARSER.declareNamedObjects(optionalConstructorArg(), INDEX_PARSER, new ParseField(INDICES));

        // ClusterHealthResponse fields
        PARSER.declareString(constructorArg(), new ParseField(CLUSTER_NAME));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_PENDING_TASKS));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_IN_FLIGHT_FETCH));
        PARSER.declareInt(constructorArg(), new ParseField(DELAYED_UNASSIGNED_SHARDS));
        PARSER.declareLong(constructorArg(), new ParseField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS));
        PARSER.declareBoolean(constructorArg(), new ParseField(TIMED_OUT));
    }

}
