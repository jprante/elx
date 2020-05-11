package org.xbib.elx.common;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.xbib.elx.api.BulkController;
import org.xbib.elx.api.ExtendedClient;
import org.xbib.elx.api.IndexAliasAdder;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexPruneResult;
import org.xbib.elx.api.IndexRetention;
import org.xbib.elx.api.IndexShiftResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractExtendedClient implements ExtendedClient {

    private static final Logger logger = LogManager.getLogger(AbstractExtendedClient.class.getName());

    private ElasticsearchClient client;

    private BulkController bulkController;

    private final AtomicBoolean closed;

    private static final IndexShiftResult EMPTY_INDEX_SHIFT_RESULT = new IndexShiftResult() {
        @Override
        public List<String> getMovedAliases() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getNewAliases() {
            return Collections.emptyList();
        }
    };

    private static final IndexPruneResult EMPTY_INDEX_PRUNE_RESULT = new IndexPruneResult() {
        @Override
        public State getState() {
            return State.NONE;
        }

        @Override
        public List<String> getCandidateIndices() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getDeletedIndices() {
            return Collections.emptyList();
        }

        @Override
        public boolean isAcknowledged() {
            return false;
        }
    };

    protected abstract ElasticsearchClient createClient(Settings settings) throws IOException;

    protected abstract void closeClient() throws IOException;

    protected AbstractExtendedClient() {
        closed = new AtomicBoolean(false);
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
    public BulkController getBulkController() {
        return bulkController;
    }

    @Override
    public AbstractExtendedClient init(Settings settings) throws IOException {
        logger.info("initializing with settings = " + settings.toDelimitedString(','));
        if (client == null) {
            client = createClient(settings);
        }
        if (bulkController == null) {
            this.bulkController = new DefaultBulkController(this);
            bulkController.init(settings);
        }
        return this;
    }

    @Override
    public void flush() throws IOException {
        if (bulkController != null) {
            bulkController.flush();
        }
    }

    @Override
    public void close() throws IOException {
        ensureClient();
        if (closed.compareAndSet(false, true)) {
            if (bulkController != null) {
                logger.info("closing bulk controller");
                bulkController.close();
            }
            closeClient();
        }
    }

    @Override
    public String getClusterName() {
        ensureClient();
        try {
            ClusterStateRequest clusterStateRequest = new ClusterStateRequest().clear();
            ClusterStateResponse clusterStateResponse =
                    client.execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
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
        ensureClient();
        waitForCluster("YELLOW", 30L, TimeUnit.SECONDS);
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
    public ExtendedClient newIndex(String index) throws IOException {
        return newIndex(index, Settings.EMPTY, (Map<String, ?>) null);
    }

    @Override
    public ExtendedClient newIndex(String index, InputStream settings, InputStream mapping) throws IOException {
        return newIndex(index,
                Settings.settingsBuilder().loadFromStream(".json", settings).build(),
                mapping != null ? JsonXContent.jsonXContent.createParser(mapping).mapOrdered() : null);
    }

    @Override
    public ExtendedClient newIndex(String index, Settings settings) throws IOException {
        return newIndex(index, settings, (Map<String, ?>) null);
    }

    @Override
    public ExtendedClient newIndex(String index, Settings settings, String mapping) throws IOException {
        return newIndex(index, settings,
                mapping != null ? JsonXContent.jsonXContent.createParser(mapping).mapOrdered() : null);
    }

    @Override
    public ExtendedClient newIndex(String index, Settings settings, XContentBuilder mapping) {
        ensureClient();
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest().index(index);
        if (settings != null) {
            createIndexRequest.settings(settings);
        }
        if (mapping != null) {
            createIndexRequest.mapping("doc", mapping);
        }
        CreateIndexResponse createIndexResponse = client.execute(CreateIndexAction.INSTANCE, createIndexRequest).actionGet();
        if (createIndexResponse.isAcknowledged()) {
            return this;
        }
        throw new IllegalStateException("index creation not acknowledged: " + index);
    }


    @Override
    public ExtendedClient newIndex(String index, Settings settings, Map<String, ?> mapping) {
        ensureClient();
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest().index(index);
        if (settings != null) {
            createIndexRequest.settings(settings);
        }
        if (mapping != null) {
            createIndexRequest.mapping("doc", mapping);
        }
        CreateIndexResponse createIndexResponse = client.execute(CreateIndexAction.INSTANCE, createIndexRequest).actionGet();
        if (createIndexResponse.isAcknowledged()) {
            return this;
        }
        throw new IllegalStateException("index creation not acknowledged: " + index);
    }

    @Override
    public ExtendedClient deleteIndex(IndexDefinition indexDefinition) {
        return deleteIndex(indexDefinition.getFullIndexName());
    }

    @Override
    public ExtendedClient deleteIndex(String index) {
        ensureClient();
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest().indices(index);
        client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
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
        if (bulkController != null) {
            ensureClient();
            bulkController.startBulkMode(index, startRefreshIntervalSeconds, stopRefreshIntervalSeconds);
        }
        return this;
    }

    @Override
    public ExtendedClient stopBulk(IndexDefinition indexDefinition) throws IOException {
        if (bulkController != null) {
            ensureClient();
            bulkController.stopBulkMode(indexDefinition);
        }
        return this;
    }

    @Override
    public ExtendedClient stopBulk(String index, long timeout, TimeUnit timeUnit) throws IOException {
        if (bulkController != null) {
            ensureClient();
            bulkController.stopBulkMode(index, timeout, timeUnit);
        }
        return this;
    }

    @Override
    public ExtendedClient index(String index, String id, boolean create, String source) {
        return index(new IndexRequest().index(index).type("doc").id(id).create(create)
                .source(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public ExtendedClient index(String index, String id, boolean create, BytesReference source) {
        return index(new IndexRequest().index(index).type("doc").id(id).create(create)
                .source(source));
    }

    @Override
    public ExtendedClient index(IndexRequest indexRequest) {
        ensureClient();
        bulkController.bulkIndex(indexRequest);
        return this;
    }

    @Override
    public ExtendedClient delete(String index, String id) {
        return delete(new DeleteRequest().index(index).type("doc").id(id));
    }

    @Override
    public ExtendedClient delete(DeleteRequest deleteRequest) {
        ensureClient();
        bulkController.bulkDelete(deleteRequest);
        return this;
    }

    @Override
    public ExtendedClient update(String index, String id, BytesReference source) {
        return update(new UpdateRequest().index(index).type("doc").id(id)
                .doc(source));
    }

    @Override
    public ExtendedClient update(String index, String id, String source) {
        return update(new UpdateRequest().index(index).type("doc").id(id)
                .doc(source.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public ExtendedClient update(UpdateRequest updateRequest) {
        ensureClient();
        bulkController.bulkUpdate(updateRequest);
        return this;
    }

    @Override
    public boolean waitForResponses(long timeout, TimeUnit timeUnit) {
        ensureClient();
        return bulkController.waitForBulkResponses(timeout, timeUnit);
    }

    @Override
    public boolean waitForRecovery(String index, long maxWaitTime, TimeUnit timeUnit) {
        ensureClient();
        ensureIndexGiven(index);
        GetSettingsRequest settingsRequest = new GetSettingsRequest();
        settingsRequest.indices(index);
        GetSettingsResponse settingsResponse = client.execute(GetSettingsAction.INSTANCE, settingsRequest).actionGet();
        int shards = settingsResponse.getIndexToSettings().get(index).getAsInt("index.number_of_shards", -1);
        if (shards > 0) {
            TimeValue timeout = toTimeValue(maxWaitTime, timeUnit);
            ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest()
                    .indices(new String[]{index})
                    .waitForActiveShards(shards)
                    .timeout(timeout);
            ClusterHealthResponse healthResponse =
                    client.execute(ClusterHealthAction.INSTANCE, clusterHealthRequest).actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                logger.warn("timeout waiting for recovery");
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean waitForCluster(String statusString, long maxWaitTime, TimeUnit timeUnit) {
        ensureClient();
        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
        TimeValue timeout = toTimeValue(maxWaitTime, timeUnit);
        ClusterHealthResponse healthResponse = client.execute(ClusterHealthAction.INSTANCE,
                new ClusterHealthRequest().timeout(timeout).waitForStatus(status)).actionGet();
        if (healthResponse != null && healthResponse.isTimedOut()) {
            logger.warn("timeout, cluster state is " + healthResponse.getStatus().name() + " and not " + status.name());
            return false;
        }
        return true;
    }

    @Override
    public String getHealthColor(long maxWaitTime, TimeUnit timeUnit) {
        ensureClient();
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
    public ExtendedClient updateReplicaLevel(IndexDefinition indexDefinition, int level) throws IOException {
        return updateReplicaLevel(indexDefinition.getFullIndexName(), level,
                indexDefinition.getMaxWaitTime(), indexDefinition.getMaxWaitTimeUnit());
    }

    @Override
    public ExtendedClient updateReplicaLevel(String index, int level, long maxWaitTime, TimeUnit timeUnit) throws IOException {
        waitForCluster("YELLOW", maxWaitTime, timeUnit); // let cluster settle down from critical operations
        if (level > 0) {
            updateIndexSetting(index, "number_of_replicas", level, maxWaitTime, timeUnit);
            waitForRecovery(index, maxWaitTime, timeUnit);
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
            ensureClient();
            client.execute(FlushAction.INSTANCE, new FlushRequest(index)).actionGet();
        }
        return this;
    }

    @Override
    public ExtendedClient refreshIndex(String index) {
        if (index != null) {
            ensureClient();
            client.execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
        return this;
    }

    @Override
    public String resolveMostRecentIndex(String alias) {
        if (alias == null) {
            return null;
        }
        ensureClient();
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases(alias);
        GetAliasesResponse getAliasesResponse = client.execute(GetAliasesAction.INSTANCE, getAliasesRequest).actionGet();
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

    public Map<String, String> getAliases(String index) {
        if (index == null) {
            return Collections.emptyMap();
        }
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(index);
        return getFilters(client.execute(GetAliasesAction.INSTANCE, getAliasesRequest).actionGet());
    }

    @Override
    public String resolveAlias(String alias) {
        ensureClient();
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.blocks(false);
        clusterStateRequest.metaData(true);
        clusterStateRequest.nodes(false);
        clusterStateRequest.routingTable(false);
        clusterStateRequest.customs(false);
        ClusterStateResponse clusterStateResponse =
                client.execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
        SortedMap<String, AliasOrIndex> map = clusterStateResponse.getState().getMetaData().getAliasAndIndexLookup();
        AliasOrIndex aliasOrIndex = map.get(alias);
        return aliasOrIndex != null ? aliasOrIndex.getIndices().iterator().next().getIndex() : null;
    }

    @Override
    public IndexShiftResult shiftIndex(IndexDefinition indexDefinition, List<String> additionalAliases) {
        return shiftIndex(indexDefinition, additionalAliases, null);
    }

    @Override
    public IndexShiftResult shiftIndex(IndexDefinition indexDefinition,
                                     List<String> additionalAliases, IndexAliasAdder indexAliasAdder) {
        if (additionalAliases == null) {
            return EMPTY_INDEX_SHIFT_RESULT;
        }
        if (indexDefinition.isShiftEnabled()) {
            return shiftIndex(indexDefinition.getIndex(),
                    indexDefinition.getFullIndexName(), additionalAliases.stream()
                            .filter(a -> a != null && !a.isEmpty())
                            .collect(Collectors.toList()), indexAliasAdder);
        }
        return EMPTY_INDEX_SHIFT_RESULT;
    }

    @Override
    public IndexShiftResult shiftIndex(String index, String fullIndexName, List<String> additionalAliases) {
        return shiftIndex(index, fullIndexName, additionalAliases, null);
    }

    @Override
    public IndexShiftResult shiftIndex(String index, String fullIndexName,
                                     List<String> additionalAliases, IndexAliasAdder adder) {
        ensureClient();
        if (index == null) {
            return EMPTY_INDEX_SHIFT_RESULT; // nothing to shift to
        }
        if (index.equals(fullIndexName)) {
            return EMPTY_INDEX_SHIFT_RESULT; // nothing to shift to
        }
        waitForCluster("YELLOW", 30L, TimeUnit.SECONDS);
        // two situations: 1. a new alias 2. there is already an old index with the alias
        String oldIndex = resolveAlias(index);
        Map<String, String> oldAliasMap = index.equals(oldIndex) ? null : getAliases(oldIndex);
        logger.debug("old index = {} old alias map = {}", oldIndex, oldAliasMap);
        final List<String> newAliases = new ArrayList<>();
        final List<String> moveAliases = new ArrayList<>();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        if (oldAliasMap == null || !oldAliasMap.containsKey(index)) {
            indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD,
                    fullIndexName, index));
            newAliases.add(index);
        }
        // move existing aliases
        if (oldAliasMap != null) {
            for (Map.Entry<String, String> entry : oldAliasMap.entrySet()) {
                String alias = entry.getKey();
                String filter = entry.getValue();
                indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.REMOVE,
                        oldIndex, alias));
                if (filter != null) {
                    indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD,
                            fullIndexName, alias).filter(filter));
                } else {
                    indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD,
                            fullIndexName, alias));
                }
                moveAliases.add(alias);
            }
        }
        // a list of aliases that should be added, check if new or old
        if (additionalAliases != null) {
            for (String additionalAlias : additionalAliases) {
                if (oldAliasMap == null || !oldAliasMap.containsKey(additionalAlias)) {
                    // index alias adder only active on extra aliases, and if alias is new
                    if (adder != null) {
                        adder.addIndexAlias(indicesAliasesRequest, fullIndexName, additionalAlias);
                    } else {
                        indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD,
                                fullIndexName, additionalAlias));
                    }
                    newAliases.add(additionalAlias);
                } else {
                    String filter = oldAliasMap.get(additionalAlias);
                    indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.REMOVE,
                            oldIndex, additionalAlias));
                    if (filter != null) {
                        indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD,
                                fullIndexName, additionalAlias).filter(filter));
                    } else {
                        indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD,
                                fullIndexName, additionalAlias));
                    }
                    moveAliases.add(additionalAlias);
                }
            }
        }
        if (!indicesAliasesRequest.getAliasActions().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (IndicesAliasesRequest.AliasActions aliasActions : indicesAliasesRequest.getAliasActions()) {
                sb.append("[").append(aliasActions.actionType().name())
                        .append(",indices=").append(Arrays.asList(aliasActions.indices()))
                        .append(",aliases=").append(Arrays.asList(aliasActions.aliases())).append("]");
            }
            logger.debug("indices alias request = {}", sb.toString());
            IndicesAliasesResponse indicesAliasesResponse =
                    client.execute(IndicesAliasesAction.INSTANCE, indicesAliasesRequest).actionGet();
            logger.debug("response isAcknowledged = {}",
                    indicesAliasesResponse.isAcknowledged());
        }
        return new SuccessIndexShiftResult(moveAliases, newAliases);
    }

    @Override
    public IndexPruneResult pruneIndex(IndexDefinition indexDefinition) {
        return pruneIndex(indexDefinition.getIndex(), indexDefinition.getFullIndexName(),
                indexDefinition.getRetention().getDelta(), indexDefinition.getRetention().getMinToKeep(), true);
    }

    @Override
    public IndexPruneResult pruneIndex(String index, String fullIndexName, int delta, int mintokeep, boolean perform) {
        if (delta == 0 && mintokeep == 0) {
            return EMPTY_INDEX_PRUNE_RESULT;
        }
        if (index.equals(fullIndexName)) {
            return EMPTY_INDEX_PRUNE_RESULT;
        }
        ensureClient();
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client, GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        logger.info("pruneIndex: total of {} indices", getIndexResponse.getIndices().length);
        List<String> candidateIndices = new ArrayList<>();
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches() && index.equals(m.group(1)) && !s.equals(fullIndexName)) {
                candidateIndices.add(s);
            }
        }
        if (candidateIndices.isEmpty()) {
            return EMPTY_INDEX_PRUNE_RESULT;
        }
        if (mintokeep > 0 && candidateIndices.size() <= mintokeep) {
            return new NothingToDoPruneResult(candidateIndices, Collections.emptyList());
        }
        List<String> indicesToDelete = new ArrayList<>();
        Matcher m1 = pattern.matcher(fullIndexName);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : candidateIndices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = candidateIndices.size() - indicesToDelete.size();
                    if ((delta == 0 || (delta > 0 && i1 - i2 >= delta)) && mintokeep <= kept) {
                        indicesToDelete.add(s);
                    }
                }
            }
        }
        if (indicesToDelete.isEmpty()) {
            return new NothingToDoPruneResult(candidateIndices, indicesToDelete);
        }
        String[] s = new String[indicesToDelete.size()];
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest()
                .indices(indicesToDelete.toArray(s));
        DeleteIndexResponse response = client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
        if (response.isAcknowledged()) {
            logger.log(Level.INFO, "deletion of {} acknowledged, waiting for GREEN", Arrays.asList(s));
            waitForCluster("GREEN", 30L, TimeUnit.SECONDS);
            return new SuccessPruneResult(candidateIndices, indicesToDelete, response);
        } else {
            logger.log(Level.WARN, "deletion of {} not acknowledged", Arrays.asList(s));
            return new FailPruneResult(candidateIndices, indicesToDelete, response);
        }
    }

    @Override
    public Long mostRecentDocument(String index, String timestampfieldname) {
        ensureClient();
        SortBuilder sort = SortBuilders
                .fieldSort(timestampfieldname)
                .order(SortOrder.DESC);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .sort(sort)
                .field(timestampfieldname)
                .size(1);
        SearchRequest searchRequest = new SearchRequest()
                .indices(index)
                .source(sourceBuilder);
        SearchResponse searchResponse = client.execute(SearchAction.INSTANCE, searchRequest).actionGet();
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
            return forceMerge(indexDefinition.getFullIndexName(), indexDefinition.getMaxWaitTime(),
                    indexDefinition.getMaxWaitTimeUnit());
        }
        return false;
    }

    @Override
    public boolean forceMerge(String index, long maxWaitTime, TimeUnit timeUnit) {
        TimeValue timeout = toTimeValue(maxWaitTime, timeUnit);
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest();
        forceMergeRequest.indices(index);
        try {
            client.execute(ForceMergeAction.INSTANCE, forceMergeRequest).get(timeout.getMillis(), TimeUnit.MILLISECONDS);
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
            // check if index name with current date already exists, resolve to it
            fullIndexName = resolveAlias(indexName + DateTimeFormatter.ofPattern(dateTimePattern)
                            .withZone(ZoneId.systemDefault()) // not GMT
                            .format(LocalDate.now()));
        } else {
            // check if index name already exists, resolve to it
            fullIndexName = resolveMostRecentIndex(indexName);
        }
        IndexRetention indexRetention = new DefaultIndexRetention()
                .setMinToKeep(settings.getAsInt("retention.mintokeep", 0))
                .setDelta(settings.getAsInt("retention.delta", 0));
        return new DefaultIndexDefinition()
                .setEnabled(isEnabled)
                .setIndex(indexName)
                .setFullIndexName(fullIndexName)
                .setSettingsUrl(settings.get("settings"))
                .setMappingsUrl(settings.get("mapping"))
                .setDateTimePattern(dateTimePattern)
                .setIgnoreErrors(settings.getAsBoolean("skiperrors", false))
                .setShift(settings.getAsBoolean("shift", true))
                .setReplicaLevel(settings.getAsInt("replica", 0))
                .setMaxWaitTime(settings.getAsLong("timeout", 30L), TimeUnit.SECONDS)
                .setRetention(indexRetention)
                .setStartRefreshInterval(settings.getAsLong("bulk.startrefreshinterval", -1L))
                .setStopRefreshInterval(settings.getAsLong("bulk.stoprefreshinterval", -1L));
    }

    @Override
    public void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) throws IOException {
        ensureClient();
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
                .settings(updateSettingsBuilder).timeout(toTimeValue(timeout, timeUnit));
        client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    private void ensureClient() {
        if (this instanceof MockExtendedClient) {
            return;
        }
        if (client == null) {
            throw new IllegalStateException("no client");
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
        ensureClient();
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
        GetMappingsResponse getMappingsResponse = client.execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
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
            SearchSourceBuilder builder = new SearchSourceBuilder()
                    .query(QueryBuilders.matchAllQuery())
                    .size(0);
            SearchRequest searchRequest = new SearchRequest()
                    .indices(index)
                    .source(builder);
            SearchResponse searchResponse =
                    client.execute(SearchAction.INSTANCE, searchRequest).actionGet();
            long total = searchResponse.getHits().getTotalHits();
            if (total > 0L) {
                Map<String, Long> fields = new TreeMap<>();
                Map<String, Object> root = mappingMetaData.getSourceAsMap();
                checkMapping(index, "", "", root, fields);
                AtomicInteger empty = new AtomicInteger();
                Map<String, Long> map = sortByValue(fields);
                map.forEach((key, value) -> {
                    logger.info("{} {} {}",
                            key, value, (double) value * 100 / total);
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
    private void checkMapping(String index,
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
                    checkMapping(index, path, key, child, fields);
                }
            } else if ("type".equals(key)) {
                QueryBuilder filterBuilder = QueryBuilders.existsQuery(path);
                QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(filterBuilder);
                SearchSourceBuilder builder = new SearchSourceBuilder()
                        .query(queryBuilder)
                        .size(0);
                SearchRequest searchRequest = new SearchRequest()
                        .indices(index)
                        .source(builder);
                SearchResponse searchResponse =
                        client.execute(SearchAction.INSTANCE, searchRequest).actionGet();
                fields.put(path, searchResponse.getHits().getTotalHits());
            }
        }
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        Map<K, V> result = new LinkedHashMap<>();
        map.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }

    private static TimeValue toTimeValue(long timeValue, TimeUnit timeUnit) {
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

    private static class SuccessIndexShiftResult implements IndexShiftResult {

        List<String> movedAliases;

        List<String> newAliases;

        SuccessIndexShiftResult(List<String> movedAliases, List<String> newAliases) {
            this.movedAliases = movedAliases;
            this.newAliases = newAliases;
        }

        @Override
        public List<String> getMovedAliases() {
            return movedAliases;
        }

        @Override
        public List<String> getNewAliases() {
            return newAliases;
        }
    }

    private static class SuccessPruneResult implements IndexPruneResult {

        List<String> candidateIndices;

        List<String> indicesToDelete;

        DeleteIndexResponse response;

        SuccessPruneResult(List<String> candidateIndices, List<String> indicesToDelete,
                           DeleteIndexResponse response) {
            this.candidateIndices = candidateIndices;
            this.indicesToDelete = indicesToDelete;
            this.response = response;
        }

        @Override
        public IndexPruneResult.State getState() {
            return IndexPruneResult.State.SUCCESS;
        }

        @Override
        public List<String> getCandidateIndices() {
            return candidateIndices;
        }

        @Override
        public List<String> getDeletedIndices() {
            return indicesToDelete;
        }

        @Override
        public boolean isAcknowledged() {
            return response.isAcknowledged();
        }
    }

    private static class FailPruneResult implements IndexPruneResult {

        List<String> candidateIndices;

        List<String> indicesToDelete;

        DeleteIndexResponse response;

        FailPruneResult(List<String> candidateIndices, List<String> indicesToDelete,
                           DeleteIndexResponse response) {
            this.candidateIndices = candidateIndices;
            this.indicesToDelete = indicesToDelete;
            this.response = response;
        }

        @Override
        public IndexPruneResult.State getState() {
            return IndexPruneResult.State.FAIL;
        }

        @Override
        public List<String> getCandidateIndices() {
            return candidateIndices;
        }

        @Override
        public List<String> getDeletedIndices() {
            return indicesToDelete;
        }

        @Override
        public boolean isAcknowledged() {
            return response.isAcknowledged();
        }
    }

    private static class NothingToDoPruneResult implements IndexPruneResult {

        List<String> candidateIndices;

        List<String> indicesToDelete;

        NothingToDoPruneResult(List<String> candidateIndices, List<String> indicesToDelete) {
            this.candidateIndices = candidateIndices;
            this.indicesToDelete = indicesToDelete;
        }

        @Override
        public IndexPruneResult.State getState() {
            return IndexPruneResult.State.SUCCESS;
        }

        @Override
        public List<String> getCandidateIndices() {
            return candidateIndices;
        }

        @Override
        public List<String> getDeletedIndices() {
            return indicesToDelete;
        }

        @Override
        public boolean isAcknowledged() {
            return false;
        }
    }
}
