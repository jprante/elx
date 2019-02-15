package org.xbib.elasticsearch.client;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractClient implements ClientMethods {

    private static final Logger logger = LogManager.getLogger(AbstractClient.class.getName());

    private Settings.Builder settingsBuilder;

    private Settings settings;

    private Map<String, String> mappings;

    private ElasticsearchClient client;

    protected BulkProcessor bulkProcessor;

    protected BulkMetric metric;

    protected BulkControl control;

    protected Throwable throwable;

    protected boolean closed;

    protected int maxActionsPerRequest = DEFAULT_MAX_ACTIONS_PER_REQUEST;

    protected int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;

    protected String maxVolumePerRequest = DEFAULT_MAX_VOLUME_PER_REQUEST;

    protected String flushIngestInterval = DEFAULT_FLUSH_INTERVAL;

    @Override
    public AbstractClient init(ElasticsearchClient client, Settings settings,
                               final BulkMetric metric, final BulkControl control) {
        this.client = client;
        this.mappings = new HashMap<>();
        if (settings == null) {
            settings = findSettings();
        }
        if (client == null && settings != null) {
            try {
                this.client = createClient(settings);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        this.metric = metric;
        this.control = control;
        if (metric != null) {
            metric.start();
        }
        resetSettings();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {

            private final Logger logger = LogManager.getLogger(getClass().getName() + ".Listener");

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = -1;
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
                long l = -1;
                if (metric != null) {
                    metric.getCurrentIngest().dec();
                    l = metric.getCurrentIngest().getCount();
                    metric.getSucceeded().inc(response.getItems().length);
                }
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (metric != null) {
                        metric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
                    }
                    if (itemResponse.isFailed()) {
                        n++;
                        if (metric != null) {
                            metric.getSucceeded().dec(1);
                            metric.getFailed().inc(1);
                        }
                    }
                }
                if (metric != null) {
                    logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] {} concurrent requests",
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
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                if (metric != null) {
                    metric.getCurrentIngest().dec();
                }
                throwable = failure;
                closed = true;
                logger.error("after bulk [" + executionId + "] error", failure);
            }
        };
        if (this.client != null) {
            BulkProcessor.Builder builder = BulkProcessor.builder(this.client, listener)
                    .setBulkActions(maxActionsPerRequest)
                    .setConcurrentRequests(maxConcurrentRequests)
                    .setFlushInterval(TimeValue.parseTimeValue(flushIngestInterval, "flushIngestInterval"));
            if (maxVolumePerRequest != null) {
                builder.setBulkSize(ByteSizeValue.parseBytesSizeValue(maxVolumePerRequest, "maxVolumePerRequest"));
            }
            this.bulkProcessor = builder.build();
        }
        this.closed = false;
        return this;
    }

    protected abstract ElasticsearchClient createClient(Settings settings) throws IOException;

    @Override
    public ElasticsearchClient client() {
        return client;
    }

    @Override
    public ClientMethods maxActionsPerRequest(int maxActionsPerRequest) {
        this.maxActionsPerRequest = maxActionsPerRequest;
        return this;
    }

    @Override
    public ClientMethods maxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    @Override
    public ClientMethods maxVolumePerRequest(String maxVolumePerRequest) {
        this.maxVolumePerRequest = maxVolumePerRequest;
        return this;
    }

    @Override
    public ClientMethods flushIngestInterval(String flushIngestInterval) {
        this.flushIngestInterval = flushIngestInterval;
        return this;
    }

    @Override
    public BulkMetric getMetric() {
        return metric;
    }

    public void resetSettings() {
        this.settingsBuilder = Settings.builder();
        settings = null;
        mappings = new HashMap<>();
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public void setting(String key, String value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.builder();
        }
        settingsBuilder.put(key, value);
    }

    public void setting(String key, Boolean value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.builder();
        }
        settingsBuilder.put(key, value);
    }

    public void setting(String key, Integer value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.builder();
        }
        settingsBuilder.put(key, value);
    }

    public void setting(InputStream in) throws IOException {
        settingsBuilder = Settings.builder().loadFromStream(".json", in, true);
    }

    public Settings.Builder settingsBuilder() {
        return settingsBuilder != null ? settingsBuilder : Settings.builder();
    }

    public Settings settings() {
        if (settings != null) {
            return settings;
        }
        if (settingsBuilder == null) {
            settingsBuilder = Settings.builder();
        }
        return settingsBuilder.build();
    }

    @Override
    public void mapping(String type, String mapping) throws IOException {
        mappings.put(type, mapping);
    }

    @Override
    public void mapping(String type, InputStream in) throws IOException {
        if (type == null) {
            return;
        }
        StringWriter sw = new StringWriter();
        Streams.copy(new InputStreamReader(in, StandardCharsets.UTF_8), sw);
        mappings.put(type, sw.toString());
    }

    @Override
    public ClientMethods index(String index, String type, String id, boolean create, BytesReference source) {
        return indexRequest(new IndexRequest(index).type(type).id(id).create(create).source(source, XContentType.JSON));
    }

    @Override
    public ClientMethods index(String index, String type, String id, boolean create, String source) {
        return indexRequest(new IndexRequest(index).type(type).id(id).create(create).source(source, XContentType.JSON));
    }

    @Override
    public ClientMethods indexRequest(IndexRequest indexRequest) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(indexRequest.index(), indexRequest.type(), indexRequest.id());
            }
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public ClientMethods delete(String index, String type, String id) {
        return deleteRequest(new DeleteRequest(index).type(type).id(id));
    }

    @Override
    public ClientMethods deleteRequest(DeleteRequest deleteRequest) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
            }
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public ClientMethods update(String index, String type, String id, BytesReference source) {
        return updateRequest(new UpdateRequest().index(index).type(type).id(id).upsert(source, XContentType.JSON));
    }

    @Override
    public ClientMethods update(String index, String type, String id, String source) {
        return updateRequest(new UpdateRequest().index(index).type(type).id(id).upsert(source, XContentType.JSON));
    }

    @Override
    public ClientMethods updateRequest(UpdateRequest updateRequest) {
        if (closed) {
            throwClose();
        }
        try {
            if (metric != null) {
                metric.getCurrentIngest().inc(updateRequest.index(), updateRequest.type(), updateRequest.id());
            }
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
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
    public ClientMethods flushIngest() {
        if (closed) {
            throwClose();
        }
        logger.debug("flushing bulk processor");
        bulkProcessor.flush();
        return this;
    }

    @Override
    public synchronized void shutdown() throws IOException {
        if (closed) {
            throwClose();
        }
        if (bulkProcessor != null) {
            logger.info("closing bulk processor...");
            bulkProcessor.close();
        }
        if (metric != null) {
            logger.info("stopping metric");
            metric.stop();
        }
        if (control != null && control.indices() != null && !control.indices().isEmpty()) {
            logger.info("stopping bulk mode for indices {}...", control.indices());
            for (String index : control.indices()) {
                stopBulk(index);
            }
        }
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
        return newIndex(index, settings(), this.mappings);
    }

    @Override
    public ClientMethods newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (closed) {
            throwClose();
        }
        if (client() == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client(), CreateIndexAction.INSTANCE).setIndex(index);
        if (settings != null) {
            logger.info("found settings {}", settings.toString());
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mappings != null) {
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                String type = entry.getKey();
                String mapping = entry.getValue();
                logger.info("found mapping for {}", type);
                createIndexRequestBuilder.addMapping(type, mapping, XContentType.JSON);
            }
        }
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created: {}", index, createIndexResponse);
        return this;
    }


    @Override
    public ClientMethods newMapping(String index, String type, Map<String, Object> mapping) {
        PutMappingRequestBuilder putMappingRequestBuilder =
                new PutMappingRequestBuilder(client(), PutMappingAction.INSTANCE)
                        .setIndices(index)
                        .setType(type)
                        .setSource(mapping);
        putMappingRequestBuilder.execute().actionGet();
        logger.info("mapping created for index {} and type {}", index, type);
        return this;
    }

    @Override
    public ClientMethods deleteIndex(String index) {
        if (closed) {
            throwClose();
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, index);
        deleteIndexRequestBuilder.execute().actionGet();
        return this;
    }

    @Override
    public ClientMethods waitForResponses(String maxWaitTime) throws InterruptedException, ExecutionException {
        if (closed) {
            throwClose();
        }
        long millis = TimeValue.parseTimeValue(maxWaitTime, "millis").getMillis();
        while (!bulkProcessor.awaitClose(millis, TimeUnit.MILLISECONDS)) {
            logger.warn("still waiting for responses");
        }
        return this;
    }

    public void waitForRecovery() throws IOException {
        if (client() == null) {
            return;
        }
        client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).actionGet();
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        if (client() == null) {
            return -1;
        }
        if (index == null) {
            throw new IOException("unable to waitfor recovery, index not set");
        }
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index)).actionGet();
        int shards = response.getTotalShards();
        client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest(index)
                .waitForActiveShards(shards)).actionGet();
        return shards;
    }

    @Override
    public void waitForCluster(String statusString, String timeout) throws IOException {
        if (client() == null) {
            return;
        }
        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
        ClusterHealthResponse healthResponse =
                client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest()
                        .waitForStatus(status).timeout(timeout)).actionGet();
        if (healthResponse != null && healthResponse.isTimedOut()) {
            throw new IOException("cluster state is " + healthResponse.getStatus().name()
                    + " and not " + status.name()
                    + ", from here on, everything will fail!");
        }
    }

    public String fetchClusterName() {
        if (client() == null) {
            return null;
        }
        try {
            ClusterStateRequestBuilder clusterStateRequestBuilder =
                    new ClusterStateRequestBuilder(client(), ClusterStateAction.INSTANCE).all();
            ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
            String name = clusterStateResponse.getClusterName().value();
            int nodeCount = clusterStateResponse.getState().getNodes().getSize();
            return name + " (" + nodeCount + " nodes connected)";
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

    public String healthColor() {
        if (client() == null) {
            return null;
        }
        try {
            ClusterHealthResponse healthResponse =
                    client().execute(ClusterHealthAction.INSTANCE,
                            new ClusterHealthRequest().timeout(TimeValue.timeValueSeconds(30))).actionGet();
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

    public int updateReplicaLevel(String index, int level) throws IOException {
        waitForCluster("YELLOW","30s");
        updateIndexSetting(index, "number_of_replicas", level);
        return waitForRecovery(index);
    }

    public void flushIndex(String index) {
        if (client() == null) {
            return;
        }
        if (index != null) {
            client().execute(FlushAction.INSTANCE, new FlushRequest(index)).actionGet();
        }
    }

    public void refreshIndex(String index) {
        if (client() == null) {
            return;
        }
        if (index != null) {
            client().execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
    }

    public void putMapping(String index) {
        if (client() == null) {
            return;
        }
        if (!mappings.isEmpty()) {
            for (Map.Entry<String, String> me : mappings.entrySet()) {
                client().execute(PutMappingAction.INSTANCE,
                        new PutMappingRequest(index).type(me.getKey()).source(me.getValue(), XContentType.JSON)).actionGet();
            }
        }
    }

    public String resolveAlias(String alias) {
        if (client() == null) {
            return alias;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }

    public String resolveMostRecentIndex(String alias) {
        if (client() == null) {
            return alias;
        }
        if (alias == null) {
            return null;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> indices = new TreeSet<>(Collections.reverseOrder());
        for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
            Matcher m = pattern.matcher(indexName.value);
            if (m.matches() && alias.equals(m.group(1))) {
                indices.add(indexName.value);
            }
        }
        return indices.isEmpty() ? alias : indices.iterator().next();
    }

    public Map<String, String> getAliasFilters(String alias) {
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        return getFilters(getAliasesRequestBuilder.setIndices(resolveAlias(alias)).execute().actionGet());
    }

    public Map<String, String> getIndexFilters(String index) {
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        return getFilters(getAliasesRequestBuilder.setIndices(index).execute().actionGet());
    }


    @Override
    public void switchAliases(String index, String concreteIndex, List<String> extraAliases) {
        switchAliases(index, concreteIndex, extraAliases, null);
    }

    @Override
    public void switchAliases(String index, String concreteIndex,
                              List<String> extraAliases, IndexAliasAdder adder) {
        if (client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        // two situations: 1. there is a new alias 2. there is already an old index with the alias
        String oldIndex = resolveAlias(index);
        final Map<String, String> oldFilterMap = oldIndex.equals(index) ? null : getIndexFilters(oldIndex);
        final List<String> newAliases = new LinkedList<>();
        final List<String> switchAliases = new LinkedList<>();
        IndicesAliasesRequestBuilder requestBuilder = new IndicesAliasesRequestBuilder(client(), IndicesAliasesAction.INSTANCE);
        if (oldFilterMap == null || !oldFilterMap.containsKey(index)) {
            // never apply a filter for trunk index name
            requestBuilder.addAlias(concreteIndex, index);
            newAliases.add(index);
        }
        // switch existing aliases
        if (oldFilterMap != null) {
            for (Map.Entry<String, String> entry : oldFilterMap.entrySet()) {
                String alias = entry.getKey();
                String filter = entry.getValue();
                requestBuilder.removeAlias(oldIndex, alias);
                if (filter != null) {
                    requestBuilder.addAlias(concreteIndex, alias, filter);
                } else {
                    requestBuilder.addAlias(concreteIndex, alias);
                }
                switchAliases.add(alias);
            }
        }
        // a list of aliases that should be added, check if new or old
        if (extraAliases != null) {
            for (String extraAlias : extraAliases) {
                if (oldFilterMap == null || !oldFilterMap.containsKey(extraAlias)) {
                    // index alias adder only active on extra aliases, and if alias is new
                    if (adder != null) {
                        adder.addIndexAlias(requestBuilder, concreteIndex, extraAlias);
                    } else {
                        requestBuilder.addAlias(concreteIndex, extraAlias);
                    }
                    newAliases.add(extraAlias);
                } else {
                    String filter = oldFilterMap.get(extraAlias);
                    requestBuilder.removeAlias(oldIndex, extraAlias);
                    if (filter != null) {
                        requestBuilder.addAlias(concreteIndex, extraAlias, filter);
                    } else {
                        requestBuilder.addAlias(concreteIndex, extraAlias);
                    }
                    switchAliases.add(extraAlias);
                }
            }
        }
        if (!newAliases.isEmpty() || !switchAliases.isEmpty()) {
            logger.info("new aliases = {}, switch aliases = {}", newAliases, switchAliases);
            requestBuilder.execute().actionGet();
        }
    }

    @Override
    public void performRetentionPolicy(String index, String concreteIndex, int timestampdiff, int mintokeep) {
        if (client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client(), GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> indices = new TreeSet<>();
        logger.info("{} indices", getIndexResponse.getIndices().length);
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches() && index.equals(m.group(1)) && !s.equals(concreteIndex)) {
                indices.add(s);
            }
        }
        if (indices.isEmpty()) {
            logger.info("no indices found, retention policy skipped");
            return;
        }
        if (mintokeep > 0 && indices.size() <= mintokeep) {
            logger.info("{} indices found, not enough for retention policy ({}),  skipped",
                    indices.size(), mintokeep);
            return;
        } else {
            logger.info("candidates for deletion = {}", indices);
        }
        List<String> indicesToDelete = new ArrayList<>();
        // our index
        Matcher m1 = pattern.matcher(concreteIndex);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : indices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = indices.size() - indicesToDelete.size();
                    if ((timestampdiff == 0 || (timestampdiff > 0 && i1 - i2 > timestampdiff)) && mintokeep <= kept) {
                        indicesToDelete.add(s);
                    }
                }
            }
        }
        logger.info("indices to delete = {}", indicesToDelete);
        if (indicesToDelete.isEmpty()) {
            logger.info("not enough indices found to delete, retention policy complete");
            return;
        }
        String[] s = indicesToDelete.toArray(new String[indicesToDelete.size()]);
        DeleteIndexRequestBuilder requestBuilder = new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, s);
        DeleteIndexResponse response = requestBuilder.execute().actionGet();
        if (!response.isAcknowledged()) {
            logger.warn("retention delete index operation was not acknowledged");
        }
    }

    @Override
    public Long mostRecentDocument(String index, String timestampfieldname) {
        if (client() == null) {
            return null;
        }
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
        SortBuilder<?> sort = SortBuilders.fieldSort(timestampfieldname).order(SortOrder.DESC);
        SearchResponse searchResponse = searchRequestBuilder.setIndices(index)
                .addStoredField(timestampfieldname)
                .setSize(1)
                .addSort(sort)
                .execute().actionGet();
        if (searchResponse.getHits().getHits().length == 1) {
            SearchHit hit = searchResponse.getHits().getHits()[0];
            if (hit.getFields().get(timestampfieldname) != null) {
                return hit.getFields().get(timestampfieldname).getValue();
            } else {
                return 0L;
            }
        }
        return null;
    }

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    protected static void throwClose() {
        throw new ElasticsearchException("client is closed");
    }


    protected void updateIndexSetting(String index, String key, Object value) throws IOException {
        if (client() == null) {
            return;
        }
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
                .settings(updateSettingsBuilder);
        client().execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    private Map<String, String> getFilters(GetAliasesResponse getAliasesResponse) {
        Map<String, String> result = new HashMap<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> object : getAliasesResponse.getAliases()) {
            List<AliasMetaData> aliasMetaDataList = object.value;
            for (AliasMetaData aliasMetaData : aliasMetaDataList) {
                if (aliasMetaData.filteringRequired()) {
                    String metaData = new String(aliasMetaData.getFilter().uncompressed(), StandardCharsets.UTF_8);
                    result.put(aliasMetaData.alias(), metaData);
                } else {
                    result.put(aliasMetaData.alias(), null);
                }
            }
        }
        return result;
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
}
