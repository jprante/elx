package org.xbib.elasticsearch.extras.client.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.Netty4Plugin;
import org.xbib.elasticsearch.extras.client.AbstractClient;
import org.xbib.elasticsearch.extras.client.BulkControl;
import org.xbib.elasticsearch.extras.client.BulkMetric;
import org.xbib.elasticsearch.extras.client.BulkProcessor;
import org.xbib.elasticsearch.extras.client.ClientMethods;
import org.xbib.elasticsearch.extras.client.NetworkUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Transport client with addtitional methods using the BulkProcessor.
 */
public class BulkTransportClient extends AbstractClient implements ClientMethods {

    private static final Logger logger = LogManager.getLogger(BulkTransportClient.class.getName());

    private int maxActionsPerRequest = DEFAULT_MAX_ACTIONS_PER_REQUEST;

    private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;

    private ByteSizeValue maxVolumePerRequest = DEFAULT_MAX_VOLUME_PER_REQUEST;

    private TimeValue flushInterval = DEFAULT_FLUSH_INTERVAL;

    private BulkProcessor bulkProcessor;

    private Throwable throwable;

    private boolean closed;

    private TransportClient client;

    private BulkMetric metric;

    private BulkControl control;

    private boolean ignoreBulkErrors;

    private boolean isShutdown;

    @Override
    public BulkTransportClient init(ElasticsearchClient client, BulkMetric metric, BulkControl control) throws IOException {
        return init(findSettings(), metric, control);
    }

    @Override
    public BulkTransportClient init(Settings settings, final BulkMetric metric, final BulkControl control) {
        createClient(settings);
        this.metric = metric;
        this.control = control;
        if (metric != null) {
            metric.start();
        }
        resetSettings();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {

            private final Logger logger = LogManager.getLogger(BulkTransportClient.class.getName() + ".Listener");

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = -1L;
                if (metric != null) {
                    metric.getCurrentIngest().inc();
                    l = metric.getCurrentIngest().getCount();
                    int n = request.numberOfActions();
                    metric.getSubmitted().inc(n);
                    metric.getCurrentIngestNumDocs().inc(n);
                    metric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                long l = -1L;
                if (metric != null) {
                    metric.getCurrentIngest().dec();
                    l = metric.getCurrentIngest().getCount();
                    metric.getSucceeded().inc(response.getItems().length);
                }
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (metric != null) {
                        metric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
                        if (itemResponse.isFailed()) {
                            n++;
                            metric.getSucceeded().dec(1);
                            metric.getFailed().inc(1);
                        }
                    }
                }
                if (metric != null) {
                    logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] [concurrent requests={}]",
                            executionId,
                            metric.getSucceeded().getCount(),
                            metric.getFailed().getCount(),
                            response.getTook().millis(),
                            l);
                }
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    if (metric != null) {
                        metric.getCurrentIngestNumDocs().dec(response.getItems().length);
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest requst, Throwable failure) {
                if (metric != null) {
                    metric.getCurrentIngest().dec();
                }
                throwable = failure;
                if (!ignoreBulkErrors) {
                    closed = true;
                }
                logger.error("bulk [" + executionId + "] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxActionsPerRequest)
                .setConcurrentRequests(maxConcurrentRequests)
                .setFlushInterval(flushInterval);
        if (maxVolumePerRequest != null) {
            builder.setBulkSize(maxVolumePerRequest);
        }
        this.bulkProcessor = builder.build();
        // auto-connect here
        try {
            Collection<InetSocketTransportAddress> addrs = findAddresses(settings);
            if (!connect(addrs, settings.getAsBoolean("autodiscover", false))) {
                throw new NoNodeAvailableException("no cluster nodes available, check settings "
                        + settings.getAsMap());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        this.closed = false;
        return this;
    }

    @Override
    public ClientMethods newMapping(String index, String type, Map<String, Object> mapping) {
        new PutMappingRequestBuilder(client(), PutMappingAction.INSTANCE)
                .setIndices(index)
                .setType(type)
                .setSource(mapping)
                .execute().actionGet();
        logger.info("mapping created for index {} and type {}", index, type);
        return this;
    }

    @Override
    protected void createClient(Settings settings) {
        if (client != null) {
            logger.warn("client is open, closing...");
            client.close();
            client.threadPool().shutdown();
            client = null;
        }
        if (settings != null) {
            String version = System.getProperty("os.name")
                    + " " + System.getProperty("java.vm.name")
                    + " " + System.getProperty("java.vm.vendor")
                    + " " + System.getProperty("java.runtime.version")
                    + " " + System.getProperty("java.vm.version");
            logger.info("creating transport client on {} with effective settings {}",
                    version, settings.getAsMap());
            this.client = new TransportClient(Settings.builder()
                    .put("cluster.name", settings.get("cluster.name"))
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                    .build(), Collections.singletonList(Netty4Plugin.class));
            this.ignoreBulkErrors = settings.getAsBoolean("ignoreBulkErrors", true);
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public BulkTransportClient maxActionsPerRequest(int maxActionsPerRequest) {
        this.maxActionsPerRequest = maxActionsPerRequest;
        return this;
    }

    @Override
    public BulkTransportClient maxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    @Override
    public BulkTransportClient maxVolumePerRequest(ByteSizeValue maxVolumePerRequest) {
        this.maxVolumePerRequest = maxVolumePerRequest;
        return this;
    }

    @Override
    public BulkTransportClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public ElasticsearchClient client() {
        return client;
    }

    @Override
    public BulkMetric getMetric() {
        return metric;
    }

    @Override
    public ClientMethods newIndex(String index) {
        if (closed) {
            throwClose();
        }
        return newIndex(index, null, null);
    }

    @Override
    public ClientMethods newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException {
        resetSettings();
        setting(settings);
        mapping(type, mappings);
        return newIndex(index, settings(), mappings());
    }

    @Override
    public ClientMethods newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (closed) {
            throwClose();
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client(), CreateIndexAction.INSTANCE).setIndex(index);
        if (settings != null) {
            logger.info("settings = {}", settings.getAsStructuredMap());
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mappings != null) {
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                String type = entry.getKey();
                String mapping = entry.getValue();
                logger.info("found mapping for {}", type);
                createIndexRequestBuilder.addMapping(type, mapping);
            }
        }
        createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created", index);
        return this;
    }

    @Override
    public ClientMethods deleteIndex(String index) {
        if (closed) {
            throwClose();
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, index).execute().actionGet();
        return this;
    }

    @Override
    public ClientMethods startBulk(String index, long startRefreshIntervalSeconds, long stopRefreshIntervalSeconds)
            throws IOException {
        if (control == null) {
            return this;
        }
        if (!control.isBulk(index) && startRefreshIntervalSeconds > 0L && stopRefreshIntervalSeconds > 0L) {
            control.startBulk(index, startRefreshIntervalSeconds, stopRefreshIntervalSeconds);
            updateIndexSetting(index, "refresh_interval", startRefreshIntervalSeconds + "s");
        }
        return this;
    }

    @Override
    public ClientMethods stopBulk(String index) throws IOException {
        if (control == null) {
            return this;
        }
        if (control.isBulk(index)) {
            long secs = control.getStopBulkRefreshIntervals().get(index);
            if (secs > 0L) {
                updateIndexSetting(index, "refresh_interval", secs + "s");
            }
            control.finishBulk(index);
        }
        return this;
    }

    @Override
    public BulkTransportClient index(String index, String type, String id, String source) {
        if (closed) {
            throwClose();
        }
        try {
            metric.getCurrentIngest().inc(index, type, id);
            bulkProcessor.add(new IndexRequest().index(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throwClose();
        }
        try {
            metric.getCurrentIngest().inc(indexRequest.index(), indexRequest.type(), indexRequest.id());
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient delete(String index, String type, String id) {
        if (closed) {
            throwClose();
        }
        try {
            metric.getCurrentIngest().inc(index, type, id);
            bulkProcessor.add(new DeleteRequest().index(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throwClose();
        }
        try {
            metric.getCurrentIngest().inc(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient update(String index, String type, String id, String source) {
        if (closed) {
            throwClose();
        }
        try {
            metric.getCurrentIngest().inc(index, type, id);
            bulkProcessor.add(new UpdateRequest().index(index).type(type).id(id).upsert(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkUpdate(UpdateRequest updateRequest) {
        if (closed) {
            throwClose();
        }
        try {
            metric.getCurrentIngest().inc(updateRequest.index(), updateRequest.type(), updateRequest.id());
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public synchronized BulkTransportClient flushIngest() {
        if (closed) {
            throwClose();
        }
        logger.debug("flushing bulk processor");
        bulkProcessor.flush();
        return this;
    }

    @Override
    public synchronized BulkTransportClient waitForResponses(TimeValue maxWaitTime)
            throws InterruptedException, ExecutionException {
        if (closed) {
            throwClose();
        }
        bulkProcessor.awaitClose(maxWaitTime.getMillis(), TimeUnit.MILLISECONDS);
        return this;
    }

    @Override
    public synchronized void shutdown() {
        if (closed) {
            shutdownClient();
            throwClose();
        }
        try {
            if (bulkProcessor != null) {
                logger.debug("closing bulk processor...");
                bulkProcessor.close();
            }
            if (control != null && control.indices() != null && !control.indices().isEmpty()) {
                logger.debug("stopping bulk mode for indices {}...", control.indices());
                for (String index : control.indices()) {
                    stopBulk(index);
                }
                metric.stop();
            }
            logger.debug("shutting down...");
            shutdownClient();
            logger.debug("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    private Settings findSettings() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("host", "localhost");
        try {
            String hostname = NetworkUtils.getLocalAddress().getHostName();
            logger.debug("the hostname is {}", hostname);
            settingsBuilder.put("host", hostname)
                    .put("port", 9300);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return settingsBuilder.build();
    }

    private Collection<InetSocketTransportAddress> findAddresses(Settings settings) throws IOException {
        String[] hostnames = settings.getAsArray("host", new String[]{"localhost"});
        int port = settings.getAsInt("port", 9300);
        Collection<InetSocketTransportAddress> addresses = new ArrayList<>();
        for (String hostname : hostnames) {
            String[] splitHost = hostname.split(":", 2);
            if (splitHost.length == 2) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                try {
                    port = Integer.parseInt(splitHost[1]);
                } catch (Exception e) {
                    logger.warn(e.getMessage(), e);
                }
                addresses.add(new InetSocketTransportAddress(inetAddress, port));
            }
            if (splitHost.length == 1) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                addresses.add(new InetSocketTransportAddress(inetAddress, port));
            }
        }
        return addresses;
    }

    private static void throwClose() {
        throw new ElasticsearchException("client is closed");
    }

    private void shutdownClient() {
        if (client != null) {
            logger.debug("shutdown started");
            client.close();
            client.threadPool().shutdown();
            client = null;
            logger.debug("shutdown complete");
        }
        isShutdown = true;
    }

    private boolean connect(Collection<InetSocketTransportAddress> addresses, boolean autodiscover) {
        logger.info("trying to connect to {}", addresses);
        client.addTransportAddresses(addresses);
        if (client.connectedNodes() != null) {
            List<DiscoveryNode> nodes = client.connectedNodes();
            if (!nodes.isEmpty()) {
                logger.info("connected to {}", nodes);
                if (autodiscover) {
                    logger.info("trying to auto-discover all cluster nodes...");
                    ClusterStateRequestBuilder clusterStateRequestBuilder =
                            new ClusterStateRequestBuilder(client, ClusterStateAction.INSTANCE);
                    ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
                    DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                    client.addDiscoveryNodes(discoveryNodes);
                    logger.info("after auto-discovery connected to {}", client.connectedNodes());
                }
                return true;
            }
            return false;
        }
        return false;
    }
}
