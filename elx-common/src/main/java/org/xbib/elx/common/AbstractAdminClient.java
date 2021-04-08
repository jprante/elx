package org.xbib.elx.common;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
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
import org.xbib.elx.api.IndexRetention;
import org.xbib.elx.api.IndexShiftResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
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

public abstract class AbstractAdminClient extends AbstractBasicClient implements AdminClient {

    private static final Logger logger = LogManager.getLogger(AbstractAdminClient.class.getName());

    /**
     * The one and only index type name used in the extended client.
     * Notr that all Elasticsearch version < 6.2.0 do not allow a prepending "_".
     */
    private static final String TYPE_NAME = "doc";

    @Override
    public Map<String, ?> getMapping(String index) throws IOException {
        GetMappingsRequestBuilder getMappingsRequestBuilder = new GetMappingsRequestBuilder(client, GetMappingsAction.INSTANCE)
                .setIndices(index)
                .setTypes(TYPE_NAME);
        GetMappingsResponse getMappingsResponse = getMappingsRequestBuilder.execute().actionGet();
        logger.info("get mappings response = {}", getMappingsResponse.getMappings().get(index).get(TYPE_NAME).getSourceAsMap());
        return getMappingsResponse.getMappings().get(index).get(TYPE_NAME).getSourceAsMap();
    }

    @Override
    public AdminClient deleteIndex(IndexDefinition indexDefinition) {
        return deleteIndex(indexDefinition.getFullIndexName());
    }

    @Override
    public AdminClient deleteIndex(String index) {
        ensureClientIsPresent();
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest().indices(index);
        client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
        waitForCluster("YELLOW", 30L, TimeUnit.SECONDS);
        return this;
    }

    @Override
    public AdminClient updateReplicaLevel(IndexDefinition indexDefinition, int level) throws IOException {
        return updateReplicaLevel(indexDefinition.getFullIndexName(), level,
                indexDefinition.getMaxWaitTime(), indexDefinition.getMaxWaitTimeUnit());
    }

    @Override
    public AdminClient updateReplicaLevel(String index, int level, long maxWaitTime, TimeUnit timeUnit) throws IOException {
        if (level < 1) {
            logger.warn("invalid replica level");
            return this;
        }
        updateIndexSetting(index, "number_of_replicas", level, maxWaitTime, timeUnit);
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
    public String resolveMostRecentIndex(String alias) {
        ensureClientIsPresent();
        if (alias == null) {
            return null;
        }
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

    @Override
    public Map<String, String> getAliases(String index) {
        if (index == null) {
            return Collections.emptyMap();
        }
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(index);
        return getFilters(client.execute(GetAliasesAction.INSTANCE, getAliasesRequest).actionGet());
    }

    @Override
    public List<String> resolveAlias(String alias) {
        if (alias == null) {
            return Collections.emptyList();
        }
        ensureClientIsPresent();
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
        return aliasOrIndex != null ? aliasOrIndex.getIndices().stream().map(IndexMetaData::getIndex).collect(Collectors.toList()) : Collections.emptyList();
    }

    @Override
    public IndexShiftResult shiftIndex(IndexDefinition indexDefinition,
                                       List<String> additionalAliases) {
        return shiftIndex(indexDefinition, additionalAliases, null);
    }

    @Override
    public IndexShiftResult shiftIndex(IndexDefinition indexDefinition,
                                       List<String> additionalAliases,
                                       IndexAliasAdder indexAliasAdder) {
        if (additionalAliases == null) {
            return new EmptyIndexShiftResult();
        }
        if (indexDefinition.isShiftEnabled()) {
            return shiftIndex(indexDefinition.getIndex(),
                    indexDefinition.getFullIndexName(), additionalAliases.stream()
                            .filter(a -> a != null && !a.isEmpty())
                            .collect(Collectors.toList()), indexAliasAdder);
        }
        return new EmptyIndexShiftResult();
    }

    private IndexShiftResult shiftIndex(String index, String fullIndexName,
                                       List<String> additionalAliases,
                                       IndexAliasAdder adder) {
        ensureClientIsPresent();
        if (index == null) {
            return new EmptyIndexShiftResult(); // nothing to shift to
        }
        if (index.equals(fullIndexName)) {
            return new EmptyIndexShiftResult(); // nothing to shift to
        }
        waitForCluster("YELLOW", 30L, TimeUnit.SECONDS);
        // two situations: 1. a new alias 2. there is already an old index with the alias
        Optional<String> oldIndex = resolveAlias(index).stream().sorted().findFirst();
        Map<String, String> oldAliasMap = oldIndex.map(this::getAliases).orElse(null);
        logger.info("old index = {} old alias map = {}", oldIndex.orElse(""), oldAliasMap);
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
                        oldIndex.get(), alias));
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
                            oldIndex.get(), additionalAlias));
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
            logger.debug("indices alias request = {}", indicesAliasesRequest.getAliasActions().toString());
            IndicesAliasesResponse indicesAliasesResponse =
                    client.execute(IndicesAliasesAction.INSTANCE, indicesAliasesRequest).actionGet();
            logger.debug("response isAcknowledged = {}",
                    indicesAliasesResponse.isAcknowledged());
        }
        return new SuccessIndexShiftResult(moveAliases, newAliases);
    }

    @Override
    public IndexPruneResult pruneIndex(IndexDefinition indexDefinition) {
        return indexDefinition.isPruneEnabled() ?
                pruneIndex(indexDefinition.getIndex(),
                indexDefinition.getFullIndexName(),
                indexDefinition.getDateTimePattern(),
                indexDefinition.getRetention().getDelta(),
                indexDefinition.getRetention().getMinToKeep()) : new EmptyPruneResult();
    }

    private IndexPruneResult pruneIndex(String index,
                                        String protectedIndexName,
                                        Pattern pattern,
                                        int delta,
                                        int mintokeep) {
        logger.info("before pruning: index = {} full index = {} delta = {} mintokeep = {} pattern = {}",
                index, protectedIndexName, delta, mintokeep, pattern);
        if (delta == 0 && mintokeep == 0) {
            return new EmptyPruneResult();
        }
        if (index.equals(protectedIndexName)) {
            return new EmptyPruneResult();
        }
        ensureClientIsPresent();
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client, GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        logger.info("before pruning: found total of {} indices", getIndexResponse.getIndices().length);
        List<String> candidateIndices = new ArrayList<>();
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches() && m.group(1).equals(index) && !s.equals(protectedIndexName)) {
                candidateIndices.add(s);
            } else {
                logger.info("not a candidate: " + s);
            }
        }
        if (candidateIndices.isEmpty()) {
            return new EmptyPruneResult();
        }
        if (mintokeep > 0 && candidateIndices.size() <= mintokeep) {
            return new NothingToDoPruneResult(candidateIndices, Collections.emptyList());
        }
        Collections.sort(candidateIndices);
        logger.info("found {} candidates", candidateIndices);
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
        logger.warn("deleting {}", indicesToDelete);
        String[] s = new String[indicesToDelete.size()];
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest()
                .indices(indicesToDelete.toArray(s));
        DeleteIndexResponse response = client.execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
        return new SuccessPruneResult(candidateIndices, indicesToDelete, response);
    }

    @Override
    public Long mostRecentDocument(String index, String timestampfieldname) {
        ensureClientIsPresent();
        SortBuilder sort = SortBuilders.fieldSort(timestampfieldname).order(SortOrder.DESC);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.sort(sort);
        builder.field(timestampfieldname);
        builder.size(1);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(builder);
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
        if (indexDefinition.isForceMergeEnabled()) {
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
        boolean isEnabled = settings.getAsBoolean("enabled", false);
        String indexName = settings.get("name", index);
        String dateTimePatternStr = settings.get("dateTimePattern", "^(.*?)(\\\\d+)$");
        Pattern dateTimePattern = Pattern.compile(dateTimePatternStr);
        String dateTimeFormat = settings.get("dateTimeFormat", "yyyyMMdd");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat)
                .withZone(ZoneId.systemDefault());
        String fullName = indexName + dateTimeFormatter.format(LocalDate.now());
        String fullIndexName = resolveAlias(fullName).stream().findFirst().orElse(fullName);
        IndexRetention indexRetention = new DefaultIndexRetention()
                .setMinToKeep(settings.getAsInt("retention.mintokeep", 0))
                .setDelta(settings.getAsInt("retention.delta", 0));
        return new DefaultIndexDefinition()
                .setEnabled(isEnabled)
                .setIndex(indexName)
                .setFullIndexName(fullIndexName)
                .setSettings(findSettingsFrom(settings.get("settings")))
                .setMappings(findMappingsFrom(settings.get("mapping")))
                .setDateTimeFormatter(dateTimeFormatter)
                .setDateTimePattern(dateTimePattern)
                .setIgnoreErrors(settings.getAsBoolean("skiperrors", false))
                .setShift(settings.getAsBoolean("shift", true))
                .setPrune(settings.getAsBoolean("prune", true))
                .setReplicaLevel(settings.getAsInt("replica", 0))
                .setMaxWaitTime(settings.getAsLong("timeout", 30L), TimeUnit.SECONDS)
                .setRetention(indexRetention)
                .setStartRefreshInterval(settings.getAsLong("bulk.startrefreshinterval", -1L))
                .setStopRefreshInterval(settings.getAsLong("bulk.stoprefreshinterval", -1L));
    }

    @Override
    public void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) throws IOException {
        ensureClientIsPresent();
        if (index == null) {
            throw new IOException("no index name given");
        }
        Settings.Builder updateSettingsBuilder = Settings.builder();
        updateSettingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(updateSettingsBuilder).timeout(toTimeValue(timeout, timeUnit));
        client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    @Override
    public void checkMapping(String index) {
        ensureClientIsPresent();
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

    private static String findSettingsFrom(String string) throws IOException {
        if (string == null) {
            return null;
        }
        try {
            URL url = new URL(string);
            try (InputStream inputStream = url.openStream()) {
                Settings settings = Settings.builder().loadFromStream(string, inputStream).build();
                XContentBuilder builder = JsonXContent.contentBuilder();
                settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return builder.string();
            }
        } catch (MalformedURLException e) {
            return string;
        }
    }

    private static String findMappingsFrom(String string) throws IOException {
        if (string == null) {
            return null;
        }
        try {
            URL url = new URL(string);
            try (InputStream inputStream = url.openStream()) {
                if (string.endsWith(".json")) {
                    Map<String, ?> mappings = JsonXContent.jsonXContent.createParser(inputStream).mapOrdered();
                    XContentBuilder builder = JsonXContent.contentBuilder();
                    builder.startObject().map(mappings).endObject();
                    return builder.string();
                }
                if (string.endsWith(".yml") || string.endsWith(".yaml")) {
                    Map<String, ?> mappings = YamlXContent.yamlXContent.createParser(inputStream).mapOrdered();
                    XContentBuilder builder = JsonXContent.contentBuilder();
                    builder.startObject().map(mappings).endObject();
                    return builder.string();
                }
            }
            return string;
        } catch (MalformedInputException e) {
            return string;
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

    private void checkMapping(String index, String type, MappingMetaData mappingMetaData) {
        try {
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices(index)
                    .setTypes(type)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0);
            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
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
                SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                        .setIndices(index)
                        .setTypes(type)
                        .setQuery(queryBuilder)
                        .setSize(0);
                SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
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

        DeleteIndexResponse response;

        SuccessPruneResult(Collection<String> candidateIndices,
                           Collection<String> indicesToDelete,
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

    private static class EmptyPruneResult implements IndexPruneResult {

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
            return "EMPTY PRUNE";
        }
    }

}
