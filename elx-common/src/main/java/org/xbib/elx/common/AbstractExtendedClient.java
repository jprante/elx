package org.xbib.elx.common;

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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
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
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.xbib.elx.api.BulkControl;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.ExtendedClient;
import org.xbib.elx.api.IndexAliasAdder;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexRetention;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractExtendedClient implements ExtendedClient {

    private static final Logger logger = LogManager.getLogger(AbstractExtendedClient.class.getName());

    /**
     * The one and only index type name used in the extended client.
     * Notr that all Elasticsearch version < 6.2.0 do not allow a prepending "_".
     */
    private static final String TYPE_NAME = "doc";

    /**
     * The Elasticsearch client.
     */
    private ElasticsearchClient client;

    /**
     * Our replacement for the buk processor.
     */
    private BulkProcessor bulkProcessor;

    private BulkMetric bulkMetric;

    private BulkControl bulkControl;

    private Throwable throwable;

    private boolean closed;

    protected abstract ElasticsearchClient createClient(Settings settings) throws IOException;

    protected AbstractExtendedClient() {
    }

    @Override
    public AbstractExtendedClient setClient(ElasticsearchClient client) {
        this.client = client;
        return this;
    }

    @Override
    public ElasticsearchClient getClient() {
        return client;
    }

    @Override
    public AbstractExtendedClient setBulkMetric(BulkMetric metric) {
        this.bulkMetric = metric;
        // you must start bulk metric or it will bail out at stop()
        bulkMetric.start();
        return this;
    }

    @Override
    public BulkMetric getBulkMetric() {
        return bulkMetric;
    }

    @Override
    public AbstractExtendedClient setBulkControl(BulkControl bulkControl) {
        this.bulkControl = bulkControl;
        return this;
    }

    @Override
    public BulkControl getBulkControl() {
        return bulkControl;
    }

    @Override
    public AbstractExtendedClient init(Settings settings) throws IOException {
        if (client == null) {
            client = createClient(settings);
        }
        if (bulkMetric != null) {
            bulkMetric.start();
        }
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {

            private final Logger logger = LogManager.getLogger("org.xbib.elx.BulkProcessor.Listener");

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = 0;
                if (bulkMetric != null) {
                    l = bulkMetric.getCurrentIngest().getCount();
                    bulkMetric.getCurrentIngest().inc();
                    int n = request.numberOfActions();
                    bulkMetric.getSubmitted().inc(n);
                    bulkMetric.getCurrentIngestNumDocs().inc(n);
                    bulkMetric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                long l = 0;
                if (bulkMetric != null) {
                    l = bulkMetric.getCurrentIngest().getCount();
                    bulkMetric.getCurrentIngest().dec();
                    bulkMetric.getSucceeded().inc(response.getItems().length);
                }
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (bulkMetric != null) {
                        bulkMetric.getCurrentIngest().dec(itemResponse.getIndex(), itemResponse.getType(), itemResponse.getId());
                    }
                    if (itemResponse.isFailed()) {
                        n++;
                        if (bulkMetric != null) {
                            bulkMetric.getSucceeded().dec(1);
                            bulkMetric.getFailed().inc(1);
                        }
                    }
                }
                if (bulkMetric != null) {
                    logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] {} concurrent requests",
                            executionId,
                            bulkMetric.getSucceeded().getCount(),
                            bulkMetric.getFailed().getCount(),
                            response.getTook().millis(),
                            l);
                }
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    if (bulkMetric != null) {
                        bulkMetric.getCurrentIngestNumDocs().dec(response.getItems().length);
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                if (bulkMetric != null) {
                    bulkMetric.getCurrentIngest().dec();
                }
                throwable = failure;
                closed = true;
                logger.error("after bulk [" + executionId + "] error", failure);
            }
        };
        if (client != null) {
            int maxActionsPerRequest = settings.getAsInt(Parameters.MAX_ACTIONS_PER_REQUEST.name(),
                    Parameters.DEFAULT_MAX_ACTIONS_PER_REQUEST.getNum());
            int maxConcurrentRequests = settings.getAsInt(Parameters.MAX_CONCURRENT_REQUESTS.name(),
                    Parameters.DEFAULT_MAX_CONCURRENT_REQUESTS.getNum());
            TimeValue flushIngestInterval = settings.getAsTime(Parameters.FLUSH_INTERVAL.name(),
                    TimeValue.timeValueSeconds(Parameters.DEFAULT_FLUSH_INTERVAL.getNum()));
            ByteSizeValue maxVolumePerRequest = settings.getAsBytesSize(Parameters.MAX_VOLUME_PER_REQUEST.name(),
                    ByteSizeValue.parseBytesSizeValue(Parameters.DEFAULT_MAX_VOLUME_PER_REQUEST.getString(),
                            "maxVolumePerRequest"));
            logger.info("bulk processor up with maxActionsPerRequest = {} maxConcurrentRequests = {} " +
                            "flushIngestInterval = {} maxVolumePerRequest = {}",
                    maxActionsPerRequest, maxConcurrentRequests, flushIngestInterval, maxVolumePerRequest);
            BulkProcessor.Builder builder = BulkProcessor.builder((Client) client, listener)
                    .setBulkActions(maxActionsPerRequest)
                    .setConcurrentRequests(maxConcurrentRequests)
                    .setFlushInterval(flushIngestInterval)
                    .setBulkSize(maxVolumePerRequest);
            this.bulkProcessor = builder.build();
        }
        this.closed = false;
        this.throwable = null;
        return this;
    }

    @Override
    public synchronized void shutdown() throws IOException {
        ensureActive();
        if (bulkProcessor != null) {
            logger.info("closing bulk processor");
            bulkProcessor.close();
        }
        if (bulkMetric != null) {
            logger.info("stopping metric before bulk stop (for precise measurement)");
            bulkMetric.stop();
        }
        if (bulkControl != null && bulkControl.indices() != null && !bulkControl.indices().isEmpty()) {
            logger.info("stopping bulk mode for indices {}...", bulkControl.indices());
            for (String index : bulkControl.indices()) {
                stopBulk(index, bulkControl.getMaxWaitTime());
            }
        }
        logger.info("shutdown complete");
    }

    @Override
    public String getClusterName() {
        ensureActive();
        try {
            ClusterStateRequestBuilder clusterStateRequestBuilder =
                    new ClusterStateRequestBuilder(client, ClusterStateAction.INSTANCE).all();
            ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
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
    public ExtendedClient newIndex(IndexDefinition indexDefinition) throws IOException {
        ensureActive();
        waitForCluster("YELLOW", "30s");
        URL indexSettings = indexDefinition.getSettingsUrl();
        if (indexSettings == null) {
            logger.warn("warning while creating index '{}', no settings/mappings",
                    indexDefinition.getFullIndexName());
            newIndex(indexDefinition.getFullIndexName());
            return this;
        }
        URL indexMappings = indexDefinition.getMappingsUrl();
        if (indexMappings == null) {
            logger.warn("warning while creating index '{}', no mappings",
                    indexDefinition.getFullIndexName());
            newIndex(indexDefinition.getFullIndexName(), indexSettings.openStream(), null);
            return this;
        }
        try (InputStream indexSettingsInput = indexSettings.openStream();
             InputStream indexMappingsInput = indexMappings.openStream()) {
            newIndex(indexDefinition.getFullIndexName(), indexSettingsInput, indexMappingsInput);
        } catch (IOException e) {
            if (indexDefinition.ignoreErrors()) {
                logger.warn(e.getMessage(), e);
                logger.warn("warning while creating index '{}' with settings at {} and mappings at {}",
                        indexDefinition.getFullIndexName(), indexSettings, indexMappings);
            } else {
                logger.error("error while creating index '{}' with settings at {} and mappings at {}",
                        indexDefinition.getFullIndexName(), indexSettings, indexMappings);
                throw new IOException(e);
            }
        }
        return this;
    }

    @Override
    public ExtendedClient newIndex(String index) {
        return newIndex(index, Settings.EMPTY, (Map<String, Object>) null);
    }

    @Override
    public ExtendedClient newIndex(String index, InputStream settings, InputStream mapping) throws IOException {
        return newIndex(index,
                Settings.settingsBuilder().loadFromStream(".json", settings).build(),
                JsonXContent.jsonXContent.createParser(mapping).mapOrdered());
    }

    @Override
    public ExtendedClient newIndex(String index, Settings settings, String mapping) throws IOException {
        return newIndex(index, settings,
                JsonXContent.jsonXContent.createParser(mapping).mapOrdered());
    }

    @Override
    public ExtendedClient newIndex(String index, Settings settings, Map<String, Object> mapping) {
        ensureActive();
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE).setIndex(index);
        if (settings != null) {
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mapping != null) {
            createIndexRequestBuilder.addMapping(TYPE_NAME, mapping);
        }
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created: {}", index, createIndexResponse);
        return this;
    }

    @Override
    public ExtendedClient deleteIndex(IndexDefinition indexDefinition) {
        return deleteIndex(indexDefinition.getFullIndexName());
    }

    @Override
    public ExtendedClient deleteIndex(String index) {
        ensureActive();
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                new DeleteIndexRequestBuilder(client, DeleteIndexAction.INSTANCE, index);
        deleteIndexRequestBuilder.execute().actionGet();
        return this;
    }

    @Override
    public ExtendedClient startBulk(IndexDefinition indexDefinition) throws IOException {
        startBulk(indexDefinition.getFullIndexName(), -1, 1);
        return this;
    }

    @Override
    public ExtendedClient startBulk(String index, long startRefreshIntervalSeconds, long stopRefreshIntervalSeconds)
            throws IOException {
        ensureActive();
        if (bulkControl == null) {
            return this;
        }
        if (!bulkControl.isBulk(index) && startRefreshIntervalSeconds > 0L && stopRefreshIntervalSeconds > 0L) {
            bulkControl.startBulk(index, startRefreshIntervalSeconds, stopRefreshIntervalSeconds);
            updateIndexSetting(index, "refresh_interval", startRefreshIntervalSeconds + "s");
        }
        return this;
    }

    @Override
    public ExtendedClient stopBulk(IndexDefinition indexDefinition) throws IOException {
        return stopBulk(indexDefinition.getFullIndexName(), indexDefinition.getMaxWaitTime());
    }

    @Override
    public ExtendedClient stopBulk(String index, String maxWaitTime) throws IOException {
        ensureActive();
        if (bulkControl == null) {
            return this;
        }
        flushIngest();
        if (waitForResponses(maxWaitTime)) {
            if (bulkControl.isBulk(index)) {
                long secs = bulkControl.getStopBulkRefreshIntervals().get(index);
                if (secs > 0L) {
                    updateIndexSetting(index, "refresh_interval", secs + "s");
                }
                bulkControl.finishBulk(index);
            }
        }
        return this;
    }

    @Override
    public ExtendedClient index(String index, String id, boolean create, BytesReference source) {
        return index(new IndexRequest(index, TYPE_NAME, id).create(create).source(source));
    }

    @Override
    public ExtendedClient index(String index, String id, boolean create, String source) {
        return index(new IndexRequest(index, TYPE_NAME, id).create(create).source(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public ExtendedClient index(IndexRequest indexRequest) {
        ensureActive();
        try {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().inc(indexRequest.index(), indexRequest.type(), indexRequest.id());
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
    public ExtendedClient delete(String index, String id) {
        return delete(new DeleteRequest(index, TYPE_NAME, id));
    }

    @Override
    public ExtendedClient delete(DeleteRequest deleteRequest) {
        ensureActive();
        try {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().inc(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
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
    public ExtendedClient update(String index, String id, BytesReference source) {
        return update(new UpdateRequest(index, TYPE_NAME, id).doc(source));
    }

    @Override
    public ExtendedClient update(String index, String id, String source) {
        return update(new UpdateRequest(index, TYPE_NAME, id).doc(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public ExtendedClient update(UpdateRequest updateRequest) {
        ensureActive();
        try {
            if (bulkMetric != null) {
                bulkMetric.getCurrentIngest().inc(updateRequest.index(), updateRequest.type(), updateRequest.id());
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
    public ExtendedClient flushIngest() {
        ensureActive();
        logger.debug("flushing bulk processor");
        bulkProcessor.flush();
        return this;
    }

    @Override
    public boolean waitForResponses(String maxWaitTime) {
        ensureActive();
        long millis = TimeValue.parseTimeValue(maxWaitTime, TimeValue.timeValueMinutes(1),"millis").getMillis();
        logger.debug("waiting for " + millis + " millis");
        try {
            return bulkProcessor.awaitFlush(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("interrupted");
            return false;
        }
    }

    @Override
    public boolean waitForRecovery(String index, String maxWaitTime) {
        ensureActive();
        ensureIndexGiven(index);
        RecoveryResponse response = client.execute(RecoveryAction.INSTANCE, new RecoveryRequest(index)).actionGet();
        int shards = response.getTotalShards();
        TimeValue timeout = TimeValue.parseTimeValue(maxWaitTime, TimeValue.timeValueSeconds(10),
                getClass().getSimpleName() + ".timeout");
        ClusterHealthResponse healthResponse =
                client.execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest(index)
                .waitForActiveShards(shards).timeout(timeout)).actionGet();
        if (healthResponse != null && healthResponse.isTimedOut()) {
            logger.error("timeout waiting for recovery");
            return false;
        }
        return true;
    }

    @Override
    public boolean waitForCluster(String statusString, String maxWaitTime) {
        ensureActive();
        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
        TimeValue timeout = TimeValue.parseTimeValue(maxWaitTime, TimeValue.timeValueSeconds(10),
                getClass().getSimpleName() + ".timeout");
        ClusterHealthResponse healthResponse = client.execute(ClusterHealthAction.INSTANCE,
                new ClusterHealthRequest().timeout(timeout).waitForStatus(status)).actionGet();
        if (healthResponse != null && healthResponse.isTimedOut()) {
            logger.error("timeout, cluster state is " + healthResponse.getStatus().name() + " and not " + status.name());
            return false;
        }
        return true;
    }

    @Override
    public String getHealthColor(String maxWaitTime) {
        ensureActive();
        try {
            TimeValue timeout = TimeValue.parseTimeValue(maxWaitTime, TimeValue.timeValueSeconds(10),
                    getClass().getSimpleName() + ".timeout");
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
    public ExtendedClient updateReplicaLevel(IndexDefinition indexDefinition, int level) throws IOException {
        return updateReplicaLevel(indexDefinition.getFullIndexName(), level, indexDefinition.getMaxWaitTime());
    }

    @Override
    public ExtendedClient updateReplicaLevel(String index, int level, String maxWaitTime) throws IOException {
        waitForCluster("YELLOW", maxWaitTime); // let cluster settle down from critical operations
        if (level > 0) {
            updateIndexSetting(index, "number_of_replicas", level);
            waitForRecovery(index, maxWaitTime);
        }
        return this;
    }

    @Override
    public int getReplicaLevel(IndexDefinition indexDefinition) {
        return getReplicaLevel(indexDefinition.getFullIndexName());
    }

    @Override
    public int getReplicaLevel(String index) {
        GetSettingsRequest request = new GetSettingsRequest().indices(index);
        GetSettingsResponse response = client.execute(GetSettingsAction.INSTANCE, request).actionGet();
        int replica = -1;
        for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
            Settings settings = cursor.value;
            if (index.equals(cursor.key)) {
                replica = settings.getAsInt("index.number_of_replicas", null);
            }
        }
        return replica;
    }

    @Override
    public ExtendedClient flushIndex(String index) {
        if (index != null) {
            ensureActive();
            client.execute(FlushAction.INSTANCE, new FlushRequest(index)).actionGet();
        }
        return this;
    }

    @Override
    public ExtendedClient refreshIndex(String index) {
        if (index != null) {
            ensureActive();
            client.execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
        return this;
    }

    @Override
    public String resolveAlias(String alias) {
        ensureActive();
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client, GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }

    @Override
    public String resolveMostRecentIndex(String alias) {
        ensureActive();
        if (alias == null) {
            return null;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client, GetAliasesAction.INSTANCE);
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

    @Override
    public Map<String, String> getAliasFilters(String alias) {
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client, GetAliasesAction.INSTANCE);
        return getFilters(getAliasesRequestBuilder.setIndices(resolveAlias(alias)).execute().actionGet());
    }

    @Override
    public Map<String, String> getIndexFilters(String index) {
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client, GetAliasesAction.INSTANCE);
        return getFilters(getAliasesRequestBuilder.setIndices(index).execute().actionGet());
    }

    @Override
    public ExtendedClient switchIndex(IndexDefinition indexDefinition, List<String> extraAliases) {
        return switchIndex(indexDefinition, extraAliases, null);
    }


    @Override
    public ExtendedClient switchIndex(IndexDefinition indexDefinition,
                            List<String> extraAliases, IndexAliasAdder indexAliasAdder) {
        if (extraAliases == null) {
            return this;
        }
        if (indexDefinition.isSwitchAliases()) {
            switchIndex(indexDefinition.getIndex(),
                    indexDefinition.getFullIndexName(), extraAliases.stream()
                            .filter(a -> a != null && !a.isEmpty())
                            .collect(Collectors.toList()), indexAliasAdder);
        }
        return this;
    }

    @Override
    public ExtendedClient switchIndex(String index, String fullIndexName, List<String> extraAliases) {
        return switchIndex(index, fullIndexName, extraAliases, null);
    }

    @Override
    public ExtendedClient switchIndex(String index, String fullIndexName,
                            List<String> extraAliases, IndexAliasAdder adder) {
        ensureActive();
        if (index.equals(fullIndexName)) {
            return this; // nothing to switch to
        }
        // two situations: 1. there is a new alias 2. there is already an old index with the alias
        String oldIndex = resolveAlias(index);
        final Map<String, String> oldFilterMap = oldIndex.equals(index) ? null : getIndexFilters(oldIndex);
        final List<String> newAliases = new LinkedList<>();
        final List<String> switchAliases = new LinkedList<>();
        IndicesAliasesRequestBuilder requestBuilder = new IndicesAliasesRequestBuilder(client, IndicesAliasesAction.INSTANCE);
        if (oldFilterMap == null || !oldFilterMap.containsKey(index)) {
            // never apply a filter for trunk index name
            requestBuilder.addAlias(fullIndexName, index);
            newAliases.add(index);
        }
        // switch existing aliases
        if (oldFilterMap != null) {
            for (Map.Entry<String, String> entry : oldFilterMap.entrySet()) {
                String alias = entry.getKey();
                String filter = entry.getValue();
                requestBuilder.removeAlias(oldIndex, alias);
                if (filter != null) {
                    requestBuilder.addAlias(fullIndexName, alias, filter);
                } else {
                    requestBuilder.addAlias(fullIndexName, alias);
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
                        adder.addIndexAlias(requestBuilder, fullIndexName, extraAlias);
                    } else {
                        requestBuilder.addAlias(fullIndexName, extraAlias);
                    }
                    newAliases.add(extraAlias);
                } else {
                    String filter = oldFilterMap.get(extraAlias);
                    requestBuilder.removeAlias(oldIndex, extraAlias);
                    if (filter != null) {
                        requestBuilder.addAlias(fullIndexName, extraAlias, filter);
                    } else {
                        requestBuilder.addAlias(fullIndexName, extraAlias);
                    }
                    switchAliases.add(extraAlias);
                }
            }
        }
        if (!newAliases.isEmpty() || !switchAliases.isEmpty()) {
            logger.info("new aliases = {}, switch aliases = {}", newAliases, switchAliases);
            requestBuilder.execute().actionGet();
        }
        return this;
    }

    @Override
    public void pruneIndex(IndexDefinition indexDefinition) {
        pruneIndex(indexDefinition.getIndex(), indexDefinition.getFullIndexName(),
                indexDefinition.getRetention().getDelta(), indexDefinition.getRetention().getMinToKeep());
    }

    @Override
    public void pruneIndex(String index, String fullIndexName, int delta, int mintokeep) {
        if (delta == 0 && mintokeep == 0) {
            return;
        }
        ensureActive();
        if (index.equals(fullIndexName)) {
            return;
        }
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client, GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> indices = new TreeSet<>();
        logger.info("{} indices", getIndexResponse.getIndices().length);
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches() && index.equals(m.group(1)) && !s.equals(fullIndexName)) {
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
        Matcher m1 = pattern.matcher(fullIndexName);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : indices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = indices.size() - indicesToDelete.size();
                    if ((delta == 0 || (delta > 0 && i1 - i2 > delta)) && mintokeep <= kept) {
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
        String[] s = new String[indicesToDelete.size()];
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest()
                .indices(indicesToDelete.toArray(s));
        DeleteIndexResponse response = client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
        if (!response.isAcknowledged()) {
            logger.warn("retention delete index operation was not acknowledged");
        }
    }

    @Override
    public Long mostRecentDocument(String index, String timestampfieldname) {
        ensureActive();
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        SortBuilder sort = SortBuilders.fieldSort(timestampfieldname).order(SortOrder.DESC);
        SearchResponse searchResponse = searchRequestBuilder.setIndices(index)
                .addField(timestampfieldname)
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
    public boolean forceMerge(IndexDefinition indexDefinition) {
        if (indexDefinition.hasForceMerge()) {
            return forceMerge(indexDefinition.getFullIndexName(), indexDefinition.getMaxWaitTime());
        }
        return false;
    }

    @Override
    public boolean forceMerge(String index, String maxWaitTime) {
        TimeValue timeout = TimeValue.parseTimeValue(maxWaitTime, TimeValue.timeValueSeconds(10),
                getClass().getSimpleName() + ".timeout");
        ForceMergeRequestBuilder forceMergeRequestBuilder =
                new ForceMergeRequestBuilder(client, ForceMergeAction.INSTANCE);
        forceMergeRequestBuilder.setIndices(index);
        try {
            forceMergeRequestBuilder.execute().get(timeout.getMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            logger.error("timeout");
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public IndexDefinition buildIndexDefinitionFromSettings(String index, Settings settings)
            throws IOException {
        boolean isEnabled = settings.getAsBoolean("enabled", !(client instanceof MockExtendedClient));
        String indexName = settings.get("name", index);
        String fullIndexName;
        String dateTimePattern = settings.get("dateTimePattern");
        if (dateTimePattern != null) {
            fullIndexName = resolveAlias(indexName +
                    DateTimeFormatter.ofPattern(dateTimePattern)
                            .withZone(ZoneId.systemDefault()) // not GMT
                            .format(LocalDate.now()));
            logger.info("index name {} resolved to full index name = {}", indexName, fullIndexName);
        } else {
            fullIndexName = resolveMostRecentIndex(indexName);
            logger.info("index name {} resolved to full index name = {}", indexName, fullIndexName);
        }
        IndexRetention indexRetention = new IndexRetention()
                .setMinToKeep(settings.getAsInt("retention.mintokeep", 0))
                .setDelta(settings.getAsInt("retention.delta", 0));

        return new IndexDefinition()
                .setIndex(indexName)
                .setFullIndexName(fullIndexName)
                .setSettingsUrl(settings.get("settings"))
                .setMappingsUrl(settings.get("mapping"))
                .setDateTimePattern(dateTimePattern)
                .setEnabled(isEnabled)
                .setIgnoreErrors(settings.getAsBoolean("skiperrors", false))
                .setSwitchAliases(settings.getAsBoolean("aliases", true))
                .setReplicaLevel(settings.getAsInt("replica", 0))
                .setMaxWaitTime(settings.get("timout", "30s"))
                .setRetention(indexRetention);
    }

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    private void updateIndexSetting(String index, String key, Object value) throws IOException {
        ensureActive();
        if (index == null) {
            throw new IOException("no index name given");
        }
        if (key == null) {
            throw new IOException("no key given");
        }
        if (value == null) {
            throw new IOException("no value given");
        }
        Settings.Builder updateSettingsBuilder = Settings.settingsBuilder();
        updateSettingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(updateSettingsBuilder);
        client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    private void ensureActive() {
        if (this instanceof MockExtendedClient) {
            return;
        }
        if (client == null) {
            throw new IllegalStateException("no client");
        }
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
    }

    private void ensureIndexGiven(String index) {
        if (index == null) {
            throw new IllegalArgumentException("no index given");
        }
    }

    private Map<String, String> getFilters(GetAliasesResponse getAliasesResponse) {
        Map<String, String> result = new HashMap<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> object : getAliasesResponse.getAliases()) {
            List<AliasMetaData> aliasMetaDataList = object.value;
            for (AliasMetaData aliasMetaData : aliasMetaDataList) {
                if (aliasMetaData.filteringRequired()) {
                    result.put(aliasMetaData.alias(),
                            new String(aliasMetaData.getFilter().uncompressed(), StandardCharsets.UTF_8));
                } else {
                    result.put(aliasMetaData.alias(), null);
                }
            }
        }
        return result;
    }

    public void checkMapping(String index) {
        ensureActive();
        GetMappingsRequestBuilder getMappingsRequestBuilder = new GetMappingsRequestBuilder(client, GetMappingsAction.INSTANCE)
                .setIndices(index);
        GetMappingsResponse getMappingsResponse = getMappingsRequestBuilder.execute().actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> map = getMappingsResponse.getMappings();
        map.keys().forEach((Consumer<ObjectCursor<String>>) stringObjectCursor -> {
            ImmutableOpenMap<String, MappingMetaData> mappings = map.get(stringObjectCursor.value);
            for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
                String mappingName = cursor.key;
                MappingMetaData mappingMetaData = cursor.value;
                checkMapping(index, mappingName, mappingMetaData);
            }
        });
    }

    private void checkMapping(String index, String type, MappingMetaData mappingMetaData) {
        try {
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
            SearchResponse searchResponse = searchRequestBuilder.setSize(0)
                    .setIndices(index)
                    .setTypes(type)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet();
            long total = searchResponse.getHits().getTotalHits();
            if (total > 0L) {
                Map<String, Long> fields = new TreeMap<>();
                Map<String, Object> root = mappingMetaData.getSourceAsMap();
                checkMapping(index, type, "", "", root, fields);
                AtomicInteger empty = new AtomicInteger();
                Map<String, Long> map = sortByValue(fields);
                map.forEach((key, value) -> {
                    logger.info("{} {} {}",
                            key,
                            value,
                            (double) value * 100 / total);
                    if (value == 0) {
                        empty.incrementAndGet();
                    }
                });
                logger.info("index={} type={} numfields={} fieldsnotused={}",
                        index, type, map.size(), empty.get());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private void checkMapping(String index, String type,
                              String pathDef, String fieldName, Map<String, Object> map,
                              Map<String, Long> fields) {
        String path = pathDef;
        if (!path.isEmpty() && !path.endsWith(".")) {
            path = path + ".";
        }
        if (!"properties".equals(fieldName)) {
            path = path + fieldName;
        }
        if (map.containsKey("index")) {
            String mode = (String) map.get("index");
            if ("no".equals(mode)) {
                return;
            }
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object o = entry.getValue();
            if (o instanceof Map) {
                Map<String, Object> child = (Map<String, Object>) o;
                o = map.get("type");
                String fieldType = o instanceof String ? o.toString() : null;
                // do not recurse into our custom field mapper
                if (!"standardnumber".equals(fieldType) && !"ref".equals(fieldType)) {
                    checkMapping(index, type, path, key, child, fields);
                }
            } else if ("type".equals(key)) {
                QueryBuilder filterBuilder = QueryBuilders.existsQuery(path);
                QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(filterBuilder);
                SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
                SearchResponse searchResponse = searchRequestBuilder.setSize(0)
                        .setIndices(index)
                        .setTypes(type)
                        .setQuery(queryBuilder)
                        .execute()
                        .actionGet();
                fields.put(path, searchResponse.getHits().totalHits());
            }
        }
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        Map<K, V> result = new LinkedHashMap<>();
        map.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue))
                .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }
}
