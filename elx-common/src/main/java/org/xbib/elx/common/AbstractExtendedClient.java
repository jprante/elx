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
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.xbib.elx.api.BulkControl;
import org.xbib.elx.api.BulkMetric;
import org.xbib.elx.api.ExtendedClient;
import org.xbib.elx.api.IndexAliasAdder;
import org.xbib.elx.common.management.IndexDefinition;
import org.xbib.elx.common.management.IndexRetention;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.MalformedURLException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public abstract class AbstractExtendedClient implements ExtendedClient {

    private static final Logger logger = LogManager.getLogger(AbstractExtendedClient.class.getName());

    private Map<String, String> mappings;

    private ElasticsearchClient client;

    private BulkProcessor bulkProcessor;

    private BulkMetric bulkMetric;

    private BulkControl bulkControl;

    private Throwable throwable;

    private boolean closed;

    private int maxActionsPerRequest;

    private int maxConcurrentRequests;

    private String maxVolumePerRequest;

    private String flushIngestInterval;

    protected abstract ElasticsearchClient createClient(Settings settings) throws IOException;

    protected AbstractExtendedClient() {
        maxActionsPerRequest = Parameters.DEFAULT_MAX_ACTIONS_PER_REQUEST.getNum();
        maxConcurrentRequests = Parameters.DEFAULT_MAX_CONCURRENT_REQUESTS.getNum();
        maxVolumePerRequest = Parameters.DEFAULT_MAX_VOLUME_PER_REQUEST.getString();
        flushIngestInterval = Parameters.DEFAULT_FLUSH_INTERVAL.getString();
        mappings = new HashMap<>();
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
            this.client = createClient(settings);
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
        if (this.client != null) {
            BulkProcessor.Builder builder = BulkProcessor.builder((Client)this.client, listener)
                    .setBulkActions(maxActionsPerRequest)
                    .setConcurrentRequests(maxConcurrentRequests)
                    .setFlushInterval(TimeValue.parseTimeValue(flushIngestInterval, null, "flushIngestInterval"));
            if (maxVolumePerRequest != null) {
                builder.setBulkSize(ByteSizeValue.parseBytesSizeValue(maxVolumePerRequest, "maxVolumePerRequest"));
            }
            this.bulkProcessor = builder.build();
        }
        this.closed = false;
        return this;
    }

    @Override
    public synchronized void shutdown() throws IOException {
        ensureActive();
        if (bulkProcessor != null) {
            logger.info("closing bulk processor...");
            bulkProcessor.close();
        }
        if (bulkMetric != null) {
            logger.info("stopping metric");
            bulkMetric.stop();
        }
        if (bulkControl != null && bulkControl.indices() != null && !bulkControl.indices().isEmpty()) {
            logger.info("stopping bulk mode for indices {}...", bulkControl.indices());
            for (String index : bulkControl.indices()) {
                stopBulk(index);
            }
        }
    }

    @Override
    public ExtendedClient maxActionsPerRequest(int maxActionsPerRequest) {
        this.maxActionsPerRequest = maxActionsPerRequest;
        return this;
    }

    @Override
    public ExtendedClient maxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    @Override
    public ExtendedClient maxVolumePerRequest(String maxVolumePerRequest) {
        this.maxVolumePerRequest = maxVolumePerRequest;
        return this;
    }

    @Override
    public ExtendedClient flushIngestInterval(String flushIngestInterval) {
        this.flushIngestInterval = flushIngestInterval;
        return this;
    }

    @Override
    public ExtendedClient newIndex(String index) {
        ensureActive();
        return newIndex(index, null, null);
    }

    @Override
    public ExtendedClient newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException {
        mapping(type, mappings);
        return newIndex(index, Settings.settingsBuilder().loadFromStream(".json", settings).build(),
                this.mappings);
    }

    @Override
    public ExtendedClient newIndex(String index, Settings settings, Map<String, String> mappings) {
        ensureActive();
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE).setIndex(index);
        if (settings != null) {
            logger.info("found settings {}", settings.getAsMap());
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
    public ExtendedClient newMapping(String index, String type, Map<String, Object> mapping) {
        PutMappingRequestBuilder putMappingRequestBuilder =
                new PutMappingRequestBuilder(client, PutMappingAction.INSTANCE)
                        .setIndices(index)
                        .setType(type)
                        .setSource(mapping);
        putMappingRequestBuilder.execute().actionGet();
        logger.info("mapping created for index {} and type {}", index, type);
        return this;
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
    public ExtendedClient stopBulk(String index) throws IOException {
        ensureActive();
        if (bulkControl == null) {
            return this;
        }
        if (bulkControl.isBulk(index)) {
            long secs = bulkControl.getStopBulkRefreshIntervals().get(index);
            if (secs > 0L) {
                updateIndexSetting(index, "refresh_interval", secs + "s");
            }
            bulkControl.finishBulk(index);
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
    public ExtendedClient waitForResponses(String maxWaitTime) throws InterruptedException {
        ensureActive();
        long millis = TimeValue.parseTimeValue(maxWaitTime, TimeValue.timeValueMinutes(1),"millis").getMillis();
        logger.debug("waiting for " + millis + " millis");
        while (!bulkProcessor.awaitClose(millis, TimeUnit.MILLISECONDS)) {
            logger.warn("still waiting for responses");
        }
        return this;
    }

    @Override
    public ExtendedClient index(String index, String type, String id, boolean create, BytesReference source) {
        return indexRequest(new IndexRequest(index).type(type).id(id).create(create).source(source));
    }

    @Override
    public ExtendedClient index(String index, String type, String id, boolean create, String source) {
        return indexRequest(new IndexRequest(index).type(type).id(id).create(create).source(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public ExtendedClient indexRequest(IndexRequest indexRequest) {
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
    public ExtendedClient delete(String index, String type, String id) {
        return deleteRequest(new DeleteRequest(index).type(type).id(id));
    }

    @Override
    public ExtendedClient deleteRequest(DeleteRequest deleteRequest) {
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
    public ExtendedClient update(String index, String type, String id, BytesReference source) {
        return updateRequest(new UpdateRequest().index(index).type(type).id(id).upsert(source));
    }

    @Override
    public ExtendedClient update(String index, String type, String id, String source) {
        return updateRequest(new UpdateRequest().index(index).type(type).id(id).upsert(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public ExtendedClient updateRequest(UpdateRequest updateRequest) {
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
    public void mapping(String type, String mapping) {
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
    public int waitForRecovery(String index) throws IOException {
        ensureActive();
        if (index == null) {
            throw new IOException("unable to wait for recovery, no index no given");
        }
        RecoveryResponse response = client.execute(RecoveryAction.INSTANCE, new RecoveryRequest(index)).actionGet();
        int shards = response.getTotalShards();
        client.execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest(index)
                .waitForActiveShards(shards)).actionGet();
        return shards;
    }

    @Override
    public void waitForCluster(String statusString, String timeout) throws IOException {
        ensureActive();
        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
        ClusterHealthResponse healthResponse =
                client.execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest()
                        .waitForStatus(status).timeout(timeout)).actionGet();
        if (healthResponse != null && healthResponse.isTimedOut()) {
            throw new IOException("cluster state is " + healthResponse.getStatus().name()
                    + " and not " + status.name()
                    + ", from here on, everything will fail!");
        }
    }

    @Override
    public String healthColor() {
        ensureActive();
        try {
            ClusterHealthResponse healthResponse =
                    client.execute(ClusterHealthAction.INSTANCE,
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

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        waitForCluster("YELLOW", "30s");
        updateIndexSetting(index, "number_of_replicas", level);
        return waitForRecovery(index);
    }

    @Override
    public void flushIndex(String index) {
        ensureActive();
        if (index != null) {
            client.execute(FlushAction.INSTANCE, new FlushRequest(index)).actionGet();
        }
    }

    @Override
    public void refreshIndex(String index) {
        ensureActive();
        if (index != null) {
            client.execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
    }

    @Override
    public void putMapping(String index) {
        ensureActive();
        if (mappings != null && !mappings.isEmpty()) {
            for (Map.Entry<String, String> me : mappings.entrySet()) {
                client.execute(PutMappingAction.INSTANCE,
                        new PutMappingRequest(index).type(me.getKey()).source(me.getValue())).actionGet();
            }
        }
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
    public void switchAliases(String index, String concreteIndex, List<String> extraAliases) {
        switchAliases(index, concreteIndex, extraAliases, null);
    }

    @Override
    public void switchAliases(String index, String concreteIndex,
                              List<String> extraAliases, IndexAliasAdder adder) {
        ensureActive();
        if (index.equals(concreteIndex)) {
            return;
        }
        // two situations: 1. there is a new alias 2. there is already an old index with the alias
        String oldIndex = resolveAlias(index);
        final Map<String, String> oldFilterMap = oldIndex.equals(index) ? null : getIndexFilters(oldIndex);
        final List<String> newAliases = new LinkedList<>();
        final List<String> switchAliases = new LinkedList<>();
        IndicesAliasesRequestBuilder requestBuilder = new IndicesAliasesRequestBuilder(client, IndicesAliasesAction.INSTANCE);
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
        if (timestampdiff == 0 && mintokeep == 0) {
            return;
        }
        ensureActive();
        if (index.equals(concreteIndex)) {
            return;
        }
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client, GetIndexAction.INSTANCE);
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
        DeleteIndexRequestBuilder requestBuilder = new DeleteIndexRequestBuilder(client, DeleteIndexAction.INSTANCE, s);
        DeleteIndexResponse response = requestBuilder.execute().actionGet();
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

    public Map<String, String> getIndexFilters(String index) {
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client, GetAliasesAction.INSTANCE);
        return getFilters(getAliasesRequestBuilder.setIndices(index).execute().actionGet());
    }

    private Map<String, String> getFilters(GetAliasesResponse getAliasesResponse) {
        Map<String, String> result = new HashMap<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> object : getAliasesResponse.getAliases()) {
            List<AliasMetaData> aliasMetaDataList = object.value;
            for (AliasMetaData aliasMetaData : aliasMetaDataList) {
                if (aliasMetaData.filteringRequired()) {
                    result.put(aliasMetaData.alias(),
                            new String(aliasMetaData.getFilter().uncompressed(), StandardCharsets.UTF_8) );
                } else {
                    result.put(aliasMetaData.alias(), null);
                }
            }
        }
        return result;
    }

    @Override
    public String getClusterName() {
        ensureActive();
        try {
            ClusterStateRequestBuilder clusterStateRequestBuilder =
                    new ClusterStateRequestBuilder(client, ClusterStateAction.INSTANCE).all();
            ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
            String name = clusterStateResponse.getClusterName().value();
            int nodeCount = clusterStateResponse.getState().getNodes().size();
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

    public IndexDefinition buildIndexDefinitionFromSettings(String index, Settings settings)
            throws MalformedURLException {
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
                .setTimestampDiff(settings.getAsInt("retention.diff", 0));
        return new IndexDefinition()
                .setIndex(indexName)
                .setFullIndexName(fullIndexName)
                .setType(settings.get("type"))
                .setSettingsUrl(new URL(settings.get("settings")))
                .setMappingsUrl(new URL(settings.get("mapping")))
                .setDateTimePattern(dateTimePattern)
                .setEnabled(isEnabled)
                .setIgnoreErrors(settings.getAsBoolean("skiperrors", false))
                .setSwitchAliases(settings.getAsBoolean("aliases", true))
                .setReplicaLevel(settings.getAsInt("replica", 0))
                .setRetention(indexRetention);
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

    @SuppressWarnings("unchecked")
    private void checkMapping(String index, String type, MappingMetaData mappingMetaData) {
        try {
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
            SearchResponse searchResponse = searchRequestBuilder.setSize(0)
                    .setIndices(index)
                    .setTypes(type)
                    .setQuery(matchAllQuery())
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
                QueryBuilder filterBuilder = existsQuery(path);
                QueryBuilder queryBuilder = constantScoreQuery(filterBuilder);
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

    @SuppressWarnings("unchecked")
    public void createIndex(IndexDefinition indexDefinition)
            throws IOException {
        ensureActive();
        waitForCluster("YELLOW", "30s");
        URL indexSettings = indexDefinition.getSettingsUrl();
        if (indexSettings == null) {
            throw new IllegalArgumentException("no settings defined for index " + indexDefinition.getIndex());
        }
        URL indexMappings = indexDefinition.getMappingsUrl();
        if (indexMappings == null) {
            throw new IllegalArgumentException("no mappings defined for index " + indexDefinition.getIndex());
        }
        try (InputStream indexSettingsInput = indexSettings.openStream();
             InputStream indexMappingsInput = indexMappings.openStream()) {
            // multiple type?
            if (indexDefinition.getType() == null) {
                Map<String, String> mapping = new HashMap<>();
                // get type names from input stream
                Reader reader = new InputStreamReader(indexMappingsInput, StandardCharsets.UTF_8);
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(reader).mapOrdered();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    mapping.put(entry.getKey(), JsonXContent.contentBuilder().map((Map<String, Object>) entry.getValue()).string());
                }
                Settings settings = Settings.settingsBuilder()
                        .loadFromStream("", indexSettingsInput)
                        .build();
                newIndex(indexDefinition.getFullIndexName(), settings, mapping);
            } else {
                newIndex(indexDefinition.getFullIndexName(),
                        indexDefinition.getType(), indexSettingsInput, indexMappingsInput);
            }
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
    }

    public void startBulk(Map<String, IndexDefinition> defs) throws IOException {
        ensureActive();
        for (Map.Entry<String, IndexDefinition> entry : defs.entrySet()) {
            IndexDefinition def = entry.getValue();
            startBulk(def.getFullIndexName(), -1, 1);
        }
    }

    public void stopBulk(Map<String, IndexDefinition> defs) throws IOException {
        ensureActive();
        if (defs == null) {
            return;
        }
        try {
            logger.info("flush bulk");
            flushIngest();
            logger.info("waiting for all bulk responses from cluster");
            waitForResponses("120s");
            logger.info("all bulk responses received");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage(), e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            logger.info("updating cluster settings of {}", defs.keySet());
            for (Map.Entry<String, IndexDefinition> entry : defs.entrySet()) {
                IndexDefinition def = entry.getValue();
                stopBulk(def.getFullIndexName());
            }
        }
    }

    public void forceMerge(Map<String, IndexDefinition> defs) {
        for (Map.Entry<String, IndexDefinition> entry : defs.entrySet()) {
            if (entry.getValue().hasForceMerge()) {
                logger.info("force merge of {}", entry.getKey());
                try {
                    ForceMergeRequestBuilder forceMergeRequestBuilder =
                            new ForceMergeRequestBuilder(client, ForceMergeAction.INSTANCE);
                    forceMergeRequestBuilder.setIndices(entry.getValue().getFullIndexName());
                    forceMergeRequestBuilder.execute().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error(e.getMessage(), e);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public void switchIndex(IndexDefinition indexDefinition, List<String> extraAliases) {
        if (extraAliases == null) {
            return;
        }
        if (indexDefinition.isSwitchAliases()) {
            // filter out null/empty values
            List<String> validAliases = extraAliases.stream()
                    .filter(a -> a != null && !a.isEmpty())
                    .collect(Collectors.toList());
            try {
                switchAliases(indexDefinition.getIndex(),
                        indexDefinition.getFullIndexName(), validAliases);
            } catch (Exception e) {
                logger.warn("switching index failed: " + e.getMessage(), e);
            }
        }
    }

    public void switchIndex(IndexDefinition indexDefinition,
                            List<String> extraAliases, IndexAliasAdder indexAliasAdder) {
        if (extraAliases == null) {
            return;
        }
        if (indexDefinition.isSwitchAliases()) {
            // filter out null/empty values
            List<String> validAliases = extraAliases.stream()
                    .filter(a -> a != null && !a.isEmpty())
                    .collect(Collectors.toList());
            try {
                switchAliases(indexDefinition.getIndex(),
                        indexDefinition.getFullIndexName(), validAliases, indexAliasAdder);
            } catch (Exception e) {
                logger.warn("switching index failed: " + e.getMessage(), e);
            }
        }
    }

    public void replica(IndexDefinition indexDefinition) {
        if (indexDefinition.getReplicaLevel() > 0) {
            try {
                updateReplicaLevel(indexDefinition.getFullIndexName(), indexDefinition.getReplicaLevel());
            } catch (Exception e) {
                logger.warn("setting replica failed: " + e.getMessage(), e);
            }
        }
    }
}
