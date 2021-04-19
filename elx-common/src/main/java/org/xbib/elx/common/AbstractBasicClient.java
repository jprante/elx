package org.xbib.elx.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.xbib.elx.api.BasicClient;
import org.xbib.elx.api.IndexDefinition;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBasicClient implements BasicClient {

    private static final Logger logger = LogManager.getLogger(AbstractBasicClient.class.getName());

    protected ElasticsearchClient client;

    protected Settings settings;

    private final ScheduledThreadPoolExecutor scheduler;

    private final AtomicBoolean closed;

    public AbstractBasicClient() {
        this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2,
                EsExecutors.daemonThreadFactory("elx-bulk-processor"));
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        closed = new AtomicBoolean(false);
    }

    @Override
    public ScheduledThreadPoolExecutor getScheduler() {
        return scheduler;
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
    public void init(Settings settings) throws IOException {
        if (closed.compareAndSet(false, true)) {
            logger.log(Level.INFO, "initializing with settings = " + settings.toDelimitedString(','));
            this.settings = settings;
            setClient(createClient(settings));
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
    public void putClusterSetting(String key, Object value, long timeout, TimeUnit timeUnit) throws IOException {
        ensureClientIsPresent();
        if (key == null) {
            throw new IOException("no key given");
        }
        if (value == null) {
            throw new IOException("no value given");
        }
        Settings.Builder updateSettingsBuilder = Settings.builder();
        updateSettingsBuilder.put(key, value.toString());
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(updateSettingsBuilder).timeout(toTimeValue(timeout, timeUnit));
        client.execute(ClusterUpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    protected Long getThreadPoolQueueSize(String name) {
        ensureClientIsPresent();
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.threadPool(true);
        NodesInfoResponse nodesInfoResponse =
                client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest).actionGet();
        for (NodeInfo nodeInfo : nodesInfoResponse.getNodes()) {
            ThreadPoolInfo threadPoolInfo = nodeInfo.getThreadPool();
            for (ThreadPool.Info info : threadPoolInfo) {
                if (info.getName().equals(name)) {
                    return info.getQueueSize().getSingles();
                }
            }
        }
        return null;
    }

    @Override
    public void waitForCluster(String statusString, long maxWaitTime, TimeUnit timeUnit) {
        ensureClientIsPresent();
        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
        TimeValue timeout = toTimeValue(maxWaitTime, timeUnit);
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest()
                .timeout(timeout)
                .waitForStatus(status);
        ClusterHealthResponse healthResponse =
                client.execute(ClusterHealthAction.INSTANCE, clusterHealthRequest).actionGet();
        if (healthResponse != null && healthResponse.isTimedOut()) {
            String message = "timeout, cluster state is " + healthResponse.getStatus().name() + " and not " + status.name();
            logger.error(message);
            throw new IllegalStateException(message);
        }
    }

    @Override
    public void waitForShards(long maxWaitTime, TimeUnit timeUnit) {
        ensureClientIsPresent();
        logger.info("waiting for cluster shard settling");
        TimeValue timeout = toTimeValue(maxWaitTime, timeUnit);
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest()
                .waitForRelocatingShards(0)
                .timeout(timeout);
        ClusterHealthResponse healthResponse =
                client.execute(ClusterHealthAction.INSTANCE, clusterHealthRequest).actionGet();
        if (healthResponse.isTimedOut()) {
            String message = "timeout waiting for cluster shards";
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
                .setSize(0);
        return searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
    }

    @Override
    public boolean isIndexExists(IndexDefinition indexDefinition) {
        ensureClientIsPresent();
        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest();
        indicesExistsRequest.indices(new String[] { indexDefinition.getFullIndexName() } );
        IndicesExistsResponse indicesExistsResponse =
                client.execute(IndicesExistsAction.INSTANCE, indicesExistsRequest).actionGet();
        return indicesExistsResponse.isExists();
    }


    @Override
    public void close() throws IOException {
        ensureClientIsPresent();
        if (closed.compareAndSet(false, true)) {
            closeClient(settings);
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    protected abstract ElasticsearchClient createClient(Settings settings) throws IOException;

    protected abstract void closeClient(Settings settings) throws IOException;

    protected void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) throws IOException {
        ensureClientIsPresent();
        if (index == null) {
            throw new IOException("no index name given");
        }
        if (key == null) {
            throw new IOException("no key given");
        }
        if (value == null) {
            throw new IOException("no value given");
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
        switch (timeUnit) {
            case DAYS:
                return TimeValue.timeValueHours(24 * timeValue);
            case HOURS:
                return TimeValue.timeValueHours(timeValue);
            case MINUTES:
                return TimeValue.timeValueMinutes(timeValue);
            case SECONDS:
                return TimeValue.timeValueSeconds(timeValue);
            case MILLISECONDS:
                return TimeValue.timeValueMillis(timeValue);
            case MICROSECONDS:
                return TimeValue.timeValueNanos(1000 * timeValue);
            case NANOSECONDS:
                return TimeValue.timeValueNanos(timeValue);
            default:
                throw new IllegalArgumentException("unknown time unit: " + timeUnit);
        }
    }
}
