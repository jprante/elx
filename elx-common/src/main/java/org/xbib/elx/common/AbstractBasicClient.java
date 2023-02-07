package org.xbib.elx.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.xbib.elx.api.BasicClient;
import org.xbib.elx.api.IndexDefinition;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBasicClient implements BasicClient {

    private static final Logger logger = LogManager.getLogger(AbstractBasicClient.class.getName());

    protected ElasticsearchClient client;

    protected Settings settings;

    private final ScheduledExecutorService executorService;

    protected final AtomicBoolean closed;

    public AbstractBasicClient() {
        this.executorService = Executors.newScheduledThreadPool(2, new DaemonThreadFactory("elx"));
        closed = new AtomicBoolean(false);
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return executorService;
    }

    @Override
    public void setClient(ElasticsearchClient client) {
        this.client = client;
    }

    @Override
    public ElasticsearchClient getClient() {
        return client;
    }

    @Override
    public boolean init(Settings settings, String infoString) throws IOException {
        if (closed.compareAndSet(false, true)) {
            this.settings = settings;
            logger.log(Level.INFO, String.format("Elx: %s on %s %s %s Java: %s %s %s %s ES: %s %s",
                    System.getProperty("user.name"),
                    System.getProperty("os.name"),
                    System.getProperty("os.arch"),
                    System.getProperty("os.version"),
                    System.getProperty("java.version"),
                    System.getProperty("java.vm.version"),
                    System.getProperty("java.vm.vendor"),
                    System.getProperty("java.vm.name"),
                    Version.CURRENT,
                    infoString));
            logger.log(Level.INFO, "initializing with settings = " + settings.toDelimitedString(','));
            setClient(createClient(settings));
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (!executorService.isShutdown()) {
                executorService.shutdownNow();
            }
            closeClient(settings);
        }
    }

    @Override
    public String getClusterName() {
        ensureClientIsPresent();
        try {
            ClusterStateRequest clusterStateRequest = new ClusterStateRequest().clear();
            ClusterStateResponse clusterStateResponse =
                    getClient().execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
            return clusterStateResponse.getClusterName().value();
        } catch (ElasticsearchTimeoutException e) {
            logger.warn(e.getMessage(), e);
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            logger.warn(e.getMessage(), e);
            return "DISCONNECTED";
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return "[" + e.getMessage() + "]";
        }
    }

    @Override
    public void putClusterSetting(String key, Object value, long timeout, TimeUnit timeUnit) {
        ensureClientIsPresent();
        if (key == null) {
            throw new IllegalArgumentException("no key given");
        }
        if (value == null) {
            throw new IllegalArgumentException("no value given");
        }
        Settings.Builder updateSettingsBuilder = Settings.builder();
        updateSettingsBuilder.put(key, value.toString());
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(updateSettingsBuilder).timeout(toTimeValue(timeout, timeUnit));
        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse =
                client.execute(ClusterUpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
        if (clusterUpdateSettingsResponse.isAcknowledged()) {
            logger.info("cluster update of " + key + " to " + value + " acknowledged");
        }
    }

    @Override
    public void waitForHealthyCluster() {
        ensureClientIsPresent();
        String statusString = settings.get(Parameters.CLUSTER_TARGET_HEALTH.getName(),
                Parameters.CLUSTER_TARGET_HEALTH.getString());
        String waitTimeStr = settings.get(Parameters.CLUSTER_TARGET_HEALTH_TIMEOUT.getName(),
                Parameters.CLUSTER_TARGET_HEALTH_TIMEOUT.getString());
        TimeValue timeValue = TimeValue.parseTimeValue(waitTimeStr, TimeValue.timeValueMinutes(30L), "");
        long maxWaitTime = timeValue.minutes();
        TimeUnit timeUnit = TimeUnit.MINUTES;
        logger.info("waiting for cluster status " + statusString + " for " + maxWaitTime + " " + timeUnit);
        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
        TimeValue timeout = toTimeValue(maxWaitTime, timeUnit);
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest()
                .timeout(timeout)
                .waitForStatus(status);
        ClusterHealthResponse healthResponse =
                client.execute(ClusterHealthAction.INSTANCE, clusterHealthRequest).actionGet();
        logger.info("got cluster status " + healthResponse.getStatus().name());
        if (healthResponse.isTimedOut()) {
            String message = "timeout, cluster state is " + healthResponse.getStatus().name() + " and not " + status.name();
            logger.error(message);
            throw new IllegalStateException(message);
        }
    }

    @Override
    public String getHealthColor(long maxWaitTime, TimeUnit timeUnit) {
        ensureClientIsPresent();
        try {
            TimeValue timeout = toTimeValue(maxWaitTime, timeUnit);
            ClusterHealthResponse healthResponse = client.execute(ClusterHealthAction.INSTANCE,
                    new ClusterHealthRequest().timeout(timeout)).actionGet();
            ClusterHealthStatus status = healthResponse.getStatus();
            return status.name();
        } catch (ElasticsearchTimeoutException e) {
            logger.warn(e.getMessage(), e);
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            logger.warn(e.getMessage(), e);
            return "DISCONNECTED";
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return "[" + e.getMessage() + "]";
        }
    }

    @Override
    public long getSearchableDocs(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return -1L;
        }
        ensureClientIsPresent();
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setIndices(indexDefinition.getFullIndexName())
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0)
                .setTrackTotalHits(true);
        return searchRequestBuilder.execute().actionGet().getHits().getTotalHits().value;
    }

    @Override
    public boolean isIndexExists(IndexDefinition indexDefinition) {
        ensureClientIsPresent();
        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest();
        indicesExistsRequest.indices(indexDefinition.getFullIndexName());
        IndicesExistsResponse indicesExistsResponse =
                client.execute(IndicesExistsAction.INSTANCE, indicesExistsRequest).actionGet();
        return indicesExistsResponse.isExists();
    }

    @Override
    public String getIndexState(IndexDefinition indexDefinition) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.blocks(false);
        clusterStateRequest.metadata(true);
        clusterStateRequest.nodes(false);
        clusterStateRequest.routingTable(false);
        clusterStateRequest.customs(false);
        ClusterStateResponse clusterStateResponse =
                client.execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
        IndexAbstraction indexAbstraction = clusterStateResponse.getState().getMetadata()
                .getIndicesLookup()
                .get(indexDefinition.getFullIndexName());
        if (indexAbstraction == null) {
            return null;
        }
        List<IndexMetadata> indexMetadata = indexAbstraction.getIndices();
        if (indexMetadata == null || indexMetadata.isEmpty()) {
            return null;
        }
        return indexMetadata.stream()
                .map(im -> im.getState().toString())
                .findFirst().get();
    }

    @Override
    public boolean isIndexClosed(IndexDefinition indexDefinition) {
        String state = getIndexState(indexDefinition);
        logger.log(Level.DEBUG, "index " + indexDefinition.getFullIndexName() + " is " + state);
        return "CLOSE".equals(state);
    }

    @Override
    public boolean isIndexOpen(IndexDefinition indexDefinition) {
        String state = getIndexState(indexDefinition);
        logger.log(Level.DEBUG, "index " + indexDefinition.getFullIndexName() + " is " + state);
        return "OPEN".equals(state);
    }

    protected abstract ElasticsearchClient createClient(Settings settings);

    protected abstract void closeClient(Settings settings);

    protected void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) {
        ensureClientIsPresent();
        if (index == null) {
            throw new IllegalArgumentException("no index name given");
        }
        if (key == null) {
            throw new IllegalArgumentException("no key given");
        }
        if (value == null) {
            throw new IllegalArgumentException("no value given");
        }
        Settings.Builder updateSettingsBuilder = Settings.builder();
        updateSettingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(updateSettingsBuilder).timeout(toTimeValue(timeout, timeUnit));
        client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    protected void ensureClientIsPresent() {
        if (client == null) {
            throw new IllegalStateException("no client");
        }
    }

    protected boolean isIndexDefinitionDisabled(IndexDefinition indexDefinition) {
        if (!indexDefinition.isEnabled()) {
            logger.warn("index " + indexDefinition.getFullIndexName() + " is disabled");
            return true;
        }
        return false;
    }

    protected static TimeValue toTimeValue(long timeValue, TimeUnit timeUnit) {
        return switch (timeUnit) {
            case DAYS -> TimeValue.timeValueHours(24 * timeValue);
            case HOURS -> TimeValue.timeValueHours(timeValue);
            case MINUTES -> TimeValue.timeValueMinutes(timeValue);
            case SECONDS -> TimeValue.timeValueSeconds(timeValue);
            case MILLISECONDS -> TimeValue.timeValueMillis(timeValue);
            case MICROSECONDS -> TimeValue.timeValueNanos(1000 * timeValue);
            case NANOSECONDS -> TimeValue.timeValueNanos(timeValue);
        };
    }
}
