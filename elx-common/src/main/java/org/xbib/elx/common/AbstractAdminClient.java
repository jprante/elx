package org.xbib.elx.common;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.SortedMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.xbib.elx.api.AdminClient;
import org.xbib.elx.api.IndexAliasAdder;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexPruneResult;
import org.xbib.elx.api.IndexShiftResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.xbib.elx.api.IndexDefinition.TYPE_NAME;

public abstract class AbstractAdminClient extends AbstractBasicClient implements AdminClient {

    private static final Logger logger = Logger.getLogger(AbstractAdminClient.class.getName());

    @Override
    public Collection<String> allIndices() {
        ensureClientIsPresent();
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.blocks(false);
        clusterStateRequest.metadata(true);
        clusterStateRequest.nodes(false);
        clusterStateRequest.routingTable(false);
        clusterStateRequest.customs(false);
        ClusterStateResponse clusterStateResponse =
                client.execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
        SortedMap<String, IndexAbstraction> indexAbstractions = clusterStateResponse.getState().getMetadata()
                .getIndicesLookup();
        if (indexAbstractions == null) {
            return Collections.emptyList();
        }
        return indexAbstractions.keySet();
    }

    @Override
    public Collection<String> allClosedIndices() {
        return allClosedIndicesOlderThan(Instant.now());
    }

    @Override
    public Collection<String> allClosedIndicesOlderThan(Instant instant) {
        ensureClientIsPresent();
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.blocks(false);
        clusterStateRequest.metadata(true);
        clusterStateRequest.nodes(false);
        clusterStateRequest.routingTable(false);
        clusterStateRequest.customs(false);
        ClusterStateResponse clusterStateResponse =
                client.execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
        return clusterStateResponse.getState().getMetadata()
                .getIndicesLookup().values().stream()
                .flatMap(ia -> ia.getIndices().stream().filter(i -> i.getState().equals(IndexMetadata.State.CLOSE) && i.getCreationDate() < instant.toEpochMilli()))
                .map(im -> im.getIndex().getName())
                .collect(Collectors.toList());
    }

    @Override
    public void purgeAllClosedIndicesOlderThan(Instant instant) {
        allClosedIndicesOlderThan(instant).forEach(this::deleteIndex);
    }

    @Override
    public Map<String, Object> getMapping(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return null;
        }
        GetMappingsRequestBuilder getMappingsRequestBuilder = new GetMappingsRequestBuilder(client, GetMappingsAction.INSTANCE)
                .setIndices(indexDefinition.getFullIndexName())
                .setTypes(TYPE_NAME);
        GetMappingsResponse getMappingsResponse = getMappingsRequestBuilder.execute().actionGet();
        return getMappingsResponse.getMappings()
                .get(indexDefinition.getFullIndexName())
                .get(TYPE_NAME)
                .getSourceAsMap();
    }

    @Override
    public void deleteIndex(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        deleteIndex(indexDefinition.getFullIndexName());
    }

    @Override
    public void deleteIndex(String indexName) {
        if (indexName == null) {
            logger.log(Level.WARNING, "no index name given to delete index");
            return;
        }
        ensureClientIsPresent();
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest().indices(indexName);
            AcknowledgedResponse acknowledgedResponse = client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
            if (acknowledgedResponse.isAcknowledged()) {
                logger.info("index " + indexName + " deleted");
            }
            waitForHealthyCluster();
        } catch (IndexNotFoundException e) {
            logger.log(Level.WARNING, "index " + indexName + " not found, skipping deletion");
        }
    }

    @Override
    public void closeIndex(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        closeIndex(indexDefinition.getFullIndexName());
    }

    @Override
    public void closeIndex(String indexName) {
        if (indexName == null) {
            logger.log(Level.WARNING, "no index name given to close index");
            return;
        }
        ensureClientIsPresent();
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest().indices(indexName);
        CloseIndexResponse closeIndexResponse = client.execute(CloseIndexAction.INSTANCE, closeIndexRequest).actionGet();
        if (closeIndexResponse.isAcknowledged()) {
            List<CloseIndexResponse.IndexResult> list = closeIndexResponse.getIndices();
            list.forEach(result -> {
                if (result.hasFailures()) {
                    logger.log(Level.WARNING, "error when closing " + result.getIndex(), result.getException());
                } else {
                    logger.log(Level.INFO, "index " + result.getIndex() + " closed");
                }
            });
        }
    }

    @Override
    public void openIndex(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        openIndex(indexDefinition.getFullIndexName());
    }

    @Override
    public void openIndex(String indexName) {
        if (indexName == null) {
            logger.log(Level.WARNING, "no index name given to close index");
            return;
        }
        ensureClientIsPresent();
        OpenIndexRequest openIndexRequest = new OpenIndexRequest().indices(indexName);
        OpenIndexResponse openIndexResponse = client.execute(OpenIndexAction.INSTANCE, openIndexRequest).actionGet();
        if (openIndexResponse.isAcknowledged()) {
            logger.log(Level.INFO, "index " + indexName + " opened");
        }
    }

    @Override
    public void updateReplicaLevel(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        if (indexDefinition.getReplicaCount() < 0) {
            logger.log(Level.WARNING, "invalid replica level defined for index "
                    + indexDefinition.getIndex() + ": " + indexDefinition.getReplicaCount());
            return;
        }
        logger.info("update replica level for " + indexDefinition + " to " + indexDefinition.getReplicaCount());
        int currentReplicaLevel = getReplicaLevel(indexDefinition);
        logger.info("current replica level for " + indexDefinition + " is " + currentReplicaLevel);
        if (currentReplicaLevel < indexDefinition.getReplicaCount()) {
            putClusterSetting("cluster.routing.allocation.node_concurrent_recoveries", "5", 30L, TimeUnit.SECONDS);
            putClusterSetting("indices.recovery.max_bytes_per_sec", "2gb", 30L, TimeUnit.SECONDS);
            logger.info("recovery boost activated");
        }
        updateIndexSetting(indexDefinition.getFullIndexName(), "number_of_replicas",
                indexDefinition.getReplicaCount(), 30L, TimeUnit.SECONDS);
        waitForHealthyCluster();
        if (currentReplicaLevel < indexDefinition.getReplicaCount()) {
            // 2 = default setting
            putClusterSetting("cluster.routing.allocation.node_concurrent_recoveries", "2", 30L, TimeUnit.SECONDS);
            // 20m = default value
            putClusterSetting("indices.recovery.max_bytes_per_sec", "40mb", 30L, TimeUnit.SECONDS);
            logger.info("recovery boost deactivated");
        }
    }

    @Override
    public int getReplicaLevel(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return -1;
        }
        ensureClientIsPresent();
        String index = indexDefinition.getFullIndexName();
        GetSettingsRequest request = new GetSettingsRequest().indices(index);
        GetSettingsResponse response = client.execute(GetSettingsAction.INSTANCE, request)
                .actionGet();
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
    public String resolveMostRecentIndex(String alias) {
        Collection<String> indices = getAlias(alias);
        return indices.isEmpty() ? alias : indices.iterator().next();
    }

    @Override
    public Map<String, String> getAliases(String index) {
        if (index == null) {
            return Collections.emptyMap();
        }
        ensureClientIsPresent();
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(index);
        return getFilters(client.execute(GetAliasesAction.INSTANCE, getAliasesRequest).actionGet());
    }


    @Override
    public Collection<String> getAlias(String alias) {
        if (alias == null) {
            return null;
        }
        ensureClientIsPresent();
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest()
                .aliases(alias);
        GetAliasesResponse getAliasesResponse = client.execute(GetAliasesAction.INSTANCE, getAliasesRequest)
                .actionGet();
        Set<String> set = new TreeSet<>();
        for (ObjectCursor<String> string : getAliasesResponse.getAliases().keys()) {
            set.add(string.value);
        }
        return set;
    }

    @Override
    public List<String> resolveAliasFromClusterState(String alias) {
        if (alias == null) {
            return Collections.emptyList();
        }
        ensureClientIsPresent();
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.blocks(false);
        clusterStateRequest.metadata(true);
        clusterStateRequest.nodes(false);
        clusterStateRequest.routingTable(false);
        clusterStateRequest.customs(false);
        ClusterStateResponse clusterStateResponse =
                client.execute(ClusterStateAction.INSTANCE, clusterStateRequest).actionGet();
        IndexAbstraction indexAbstraction = clusterStateResponse.getState().getMetadata()
                .getIndicesLookup().get(alias);
        if (indexAbstraction == null) {
            return Collections.emptyList();
        }
        List<IndexMetadata> indexMetadata = indexAbstraction.getIndices();
        if (indexMetadata == null) {
            return Collections.emptyList();
        }
        return indexMetadata.stream()
                .map(im -> im.getIndex().getName())
                .sorted() // important
                .collect(Collectors.toList());
    }

    @Override
    public IndexShiftResult shiftIndex(IndexDefinition indexDefinition,
                                       Collection<String> additionalAliases,
                                       IndexAliasAdder indexAliasAdder) {
        if (additionalAliases == null) {
            return new EmptyIndexShiftResult();
        }
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return new EmptyIndexShiftResult();
        }
        if (indexDefinition.isShiftEnabled()) {
            if (indexDefinition.isCloseShifted()) {
                getAlias(indexDefinition.getIndex()).stream()
                        .filter(s -> !s.equals(indexDefinition.getFullIndexName()))
                        .forEach(this::closeIndex);
            }
            return shiftIndex(indexDefinition.getIndex(), indexDefinition.getFullIndexName(),
                    additionalAliases.stream()
                            .filter(a -> a != null && !a.isEmpty())
                            .collect(Collectors.toList()), indexAliasAdder);
        }
        return new EmptyIndexShiftResult();
    }

    private IndexShiftResult shiftIndex(String index,
                                        String fullIndexName,
                                        List<String> additionalAliases,
                                        IndexAliasAdder adder) {
        ensureClientIsPresent();
        if (index == null) {
            return new EmptyIndexShiftResult(); // nothing to shift to
        }
        if (index.equals(fullIndexName)) {
            return new EmptyIndexShiftResult(); // nothing to shift to
        }
        // two situations: 1. a new alias 2. there is already an old index with the alias
        Optional<String> oldIndex = resolveAliasFromClusterState(index).stream().sorted().findFirst();
        Map<String, String> oldAliasMap = oldIndex.map(this::getAliases).orElse(null);
        logger.log(Level.INFO, "old index = " + oldIndex.orElse("") + " old alias map = " + oldAliasMap);
        final List<String> newAliases = new ArrayList<>();
        final List<String> moveAliases = new ArrayList<>();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        if (oldAliasMap == null || !oldAliasMap.containsKey(index)) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                    .index(fullIndexName).alias(index));
            newAliases.add(index);
        }
        // move existing aliases
        if (oldIndex.isPresent() && oldAliasMap != null) {
            for (Map.Entry<String, String> entry : oldAliasMap.entrySet()) {
                String alias = entry.getKey();
                String filter = entry.getValue();
                indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove()
                        .index(oldIndex.get()).alias(alias));
                if (filter != null) {
                    indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove()
                            .index(fullIndexName).alias(alias).filter(filter));
                } else {
                    indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                            .index(fullIndexName).alias(alias));
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
                        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                                .index(fullIndexName)
                                .alias(additionalAlias)
                                .filter(adder.addAliasOnField(fullIndexName, additionalAlias)));
                    } else {
                        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                                .index(fullIndexName)
                                .alias(additionalAlias));
                    }
                    newAliases.add(additionalAlias);
                } else {
                    String filter = oldAliasMap.get(additionalAlias);
                    indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove()
                            .index(oldIndex.get()).alias(additionalAlias));
                    if (filter != null) {
                        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                                .index(fullIndexName).alias(additionalAlias).filter(filter));
                    } else {
                        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                                .index(fullIndexName).alias(additionalAlias));
                    }
                    moveAliases.add(additionalAlias);
                }
            }
        }
        if (!indicesAliasesRequest.getAliasActions().isEmpty()) {
            client.execute(IndicesAliasesAction.INSTANCE, indicesAliasesRequest).actionGet();
        }
        return new SuccessIndexShiftResult(moveAliases, newAliases);
    }

    @Override
    public IndexPruneResult pruneIndex(IndexDefinition indexDefinition) {
        return indexDefinition != null &&
                indexDefinition.isEnabled() &&
                indexDefinition.isPruneEnabled() &&
                indexDefinition.getDateTimePattern() != null ?
                pruneIndex(indexDefinition.getIndex(),
                indexDefinition.getFullIndexName(),
                indexDefinition.getDateTimePattern(),
                indexDefinition.getDelta(),
                indexDefinition.getMinToKeep()) : new NonePruneResult();
    }

    private IndexPruneResult pruneIndex(String index,
                                        String protectedIndexName,
                                        Pattern pattern,
                                        int delta,
                                        int mintokeep) {
        logger.log(Level.INFO, MessageFormat.format("before pruning: index = {0} full index = {1} delta = {2} mintokeep = {3} pattern = {4}",
                index, protectedIndexName, delta, mintokeep, pattern));
        if (delta == 0 && mintokeep == 0) {
            logger.log(Level.INFO, "no candidates found, delta is 0 and mintokeep is 0");
            return new NonePruneResult();
        }
        if (index.equals(protectedIndexName)) {
            logger.log(Level.INFO, "no candidates found, only protected index name is given");
            return new NonePruneResult();
        }
        ensureClientIsPresent();
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client, GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        logger.log(Level.INFO, "before pruning: found total of " + getIndexResponse.getIndices().length + " indices");
        List<String> candidateIndices = new ArrayList<>();
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches() && m.group(1).equals(index) && !s.equals(protectedIndexName)) {
                candidateIndices.add(s);
            }
        }
        if (candidateIndices.isEmpty()) {
            logger.info("no candidates found to prune");
            return new NonePruneResult();
        }
        if (mintokeep > 0 && candidateIndices.size() <= mintokeep) {
            return new NothingToDoPruneResult(candidateIndices, Collections.emptyList());
        }
        Collections.sort(candidateIndices);
        logger.log(Level.INFO, "found candidates: " + candidateIndices);
        List<String> indicesToDelete = new ArrayList<>();
        Matcher m1 = pattern.matcher(protectedIndexName);
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
        logger.log(Level.INFO, "deleting " + indicesToDelete);
        String[] s = new String[indicesToDelete.size()];
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest()
                .indices(indicesToDelete.toArray(s));
        AcknowledgedResponse response = client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
        return new SuccessPruneResult(candidateIndices, indicesToDelete, response);
    }

    @Override
    public Long mostRecentDocument(IndexDefinition indexDefinition, String timestampfieldname) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return -1L;
        }
        ensureClientIsPresent();
        SortBuilder<?> sort = SortBuilders.fieldSort(timestampfieldname).order(SortOrder.DESC);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.sort(sort);
        builder.storedField(timestampfieldname);
        builder.size(1);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexDefinition.getFullIndexName());
        searchRequest.source(builder);
        SearchResponse searchResponse =
                client.execute(SearchAction.INSTANCE, searchRequest).actionGet();
        if (searchResponse.getHits().getHits().length == 1) {
            SearchHit hit = searchResponse.getHits().getHits()[0];
            if (hit.getFields().get(timestampfieldname) != null) {
                return hit.getFields().get(timestampfieldname).getValue();
            } else {
                return 0L;
            }
        }
        // almost impossible
        return -1L;
    }

    @Override
    public void forceMerge(IndexDefinition indexDefinition, int maxNumSegments) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        if (!indexDefinition.isForceMergeEnabled()) {
            logger.info("force merge is disabled (this is the default in an index definition)");
            return;
        }
        forceMerge(indexDefinition.getFullIndexName(), maxNumSegments);
    }

    @Override
    public void forceMerge(String indexName, int maxNumSegments) {
        ensureClientIsPresent();
        logger.log(Level.INFO, "starting force merge of " + indexName + " to " + maxNumSegments + " segments");
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest();
        forceMergeRequest.indices(indexName);
        forceMergeRequest.maxNumSegments(maxNumSegments);
        ForceMergeResponse forceMergeResponse = client.execute(ForceMergeAction.INSTANCE, forceMergeRequest)
                    .actionGet();
        logger.log(Level.INFO, "after force merge, status = " + forceMergeResponse.getStatus());
        if (forceMergeResponse.getFailedShards() > 0) {
            throw new IllegalStateException("failed shards after force merge: " + forceMergeResponse.getFailedShards());
        }
        waitForHealthyCluster();
    }

    @Override
    public void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) {
        ensureClientIsPresent();
        Settings.Builder updateSettingsBuilder = Settings.builder();
        updateSettingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(updateSettingsBuilder).timeout(toTimeValue(timeout, timeUnit));
        client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
        waitForHealthyCluster();
    }

    @Override
    public void checkMapping(IndexDefinition indexDefinition) {
        if (isIndexDefinitionDisabled(indexDefinition)) {
            return;
        }
        ensureClientIsPresent();
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(indexDefinition.getFullIndexName());
        GetMappingsResponse getMappingsResponse = client.execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> map = getMappingsResponse.getMappings();
        map.keys().forEach((Consumer<ObjectCursor<String>>) stringObjectCursor -> {
            ImmutableOpenMap<String, MappingMetadata> mappings = map.get(stringObjectCursor.value);
            for (ObjectObjectCursor<String, MappingMetadata> cursor : mappings) {
                MappingMetadata mappingMetaData = cursor.value;
                checkMapping(indexDefinition.getFullIndexName(), mappingMetaData);
            }
        });
    }

    private Map<String, String> getFilters(GetAliasesResponse getAliasesResponse) {
        Map<String, String> result = new HashMap<>();
        for (ObjectObjectCursor<String, List<AliasMetadata>> object : getAliasesResponse.getAliases()) {
            List<AliasMetadata> aliasMetadataList = object.value;
            for (AliasMetadata aliasMetadata : aliasMetadataList) {
                if (aliasMetadata.filteringRequired()) {
                    result.put(aliasMetadata.alias(),aliasMetadata.getFilter().string());
                } else {
                    result.put(aliasMetadata.alias(), null);
                }
            }
        }
        return result;
    }

    private void checkMapping(String index, MappingMetadata mappingMetaData) {
        try {
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices(index)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0)
                    .setTrackTotalHits(true);
            SearchResponse searchResponse =
                    searchRequestBuilder.execute().actionGet();
            long total = searchResponse.getHits().getTotalHits().value;
            if (total > 0L) {
                Map<String, Long> fields = new TreeMap<>();
                Map<String, Object> root = mappingMetaData.getSourceAsMap();
                checkMapping(index, "", "", root, fields);
                AtomicInteger empty = new AtomicInteger();
                Map<String, Long> map = sortByValue(fields);
                map.forEach((key, value) -> {
                    logger.log(Level.INFO, MessageFormat.format("{0} {1} {2}",
                            key,
                            value,
                            (double) value * 100 / total));
                    if (value == 0) {
                        empty.incrementAndGet();
                    }
                });
                logger.log(Level.INFO, MessageFormat.format("index = {0} numfields = {1} fieldsnotused = {2}",
                        index, map.size(), empty.get()));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
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
            Object mode = map.get("index");
            if (mode instanceof String) {
                if ("no".equals(mode)) {
                    return;
                }
            }
            if (mode instanceof Boolean b) {
                if (!b) {
                    return;
                }
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
                SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                        .setIndices(index)
                        .setQuery(queryBuilder)
                        .setSize(0)
                        .setTrackTotalHits(true);
                SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
                fields.put(path, searchResponse.getHits().getTotalHits().value);
            }
        }
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        Map<K, V> result = new LinkedHashMap<>();
        map.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }

    private static class SuccessIndexShiftResult implements IndexShiftResult {

        Collection<String> movedAliases;

        Collection<String> newAliases;

        SuccessIndexShiftResult(Collection<String> movedAliases, Collection<String> newAliases) {
            this.movedAliases = movedAliases;
            this.newAliases = newAliases;
        }

        @Override
        public Collection<String> getMovedAliases() {
            return movedAliases;
        }

        @Override
        public Collection<String> getNewAliases() {
            return newAliases;
        }
    }

    private static class EmptyIndexShiftResult implements IndexShiftResult {

        @Override
        public List<String> getMovedAliases() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getNewAliases() {
            return Collections.emptyList();
        }
    }

    private static class SuccessPruneResult implements IndexPruneResult {

        Collection<String> candidateIndices;

        Collection<String> indicesToDelete;

        AcknowledgedResponse response;

        SuccessPruneResult(Collection<String> candidateIndices,
                           Collection<String> indicesToDelete,
                           AcknowledgedResponse response) {
            this.candidateIndices = candidateIndices;
            this.indicesToDelete = indicesToDelete;
            this.response = response;
        }

        @Override
        public IndexPruneResult.State getState() {
            return IndexPruneResult.State.SUCCESS;
        }

        @Override
        public Collection<String> getCandidateIndices() {
            return candidateIndices;
        }

        @Override
        public Collection<String> getDeletedIndices() {
            return indicesToDelete;
        }

        @Override
        public boolean isAcknowledged() {
            return response.isAcknowledged();
        }

        @Override
        public String toString() {
            return "PRUNED: " + indicesToDelete;
        }
    }

    private static class NothingToDoPruneResult implements IndexPruneResult {

        Collection<String> candidateIndices;

        Collection<String> indicesToDelete;

        NothingToDoPruneResult(Collection<String> candidateIndices, List<String> indicesToDelete) {
            this.candidateIndices = candidateIndices;
            this.indicesToDelete = indicesToDelete;
        }

        @Override
        public IndexPruneResult.State getState() {
            return IndexPruneResult.State.SUCCESS;
        }

        @Override
        public Collection<String> getCandidateIndices() {
            return candidateIndices;
        }

        @Override
        public Collection<String> getDeletedIndices() {
            return indicesToDelete;
        }

        @Override
        public boolean isAcknowledged() {
            return false;
        }

        @Override
        public String toString() {
            return "NOTHING TO DO";
        }
    }

    private static class NonePruneResult implements IndexPruneResult {

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

        @Override
        public String toString() {
            return "NONE";
        }
    }
}
