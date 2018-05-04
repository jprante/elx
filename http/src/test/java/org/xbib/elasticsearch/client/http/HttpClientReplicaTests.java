package org.xbib.elasticsearch.client.http;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=3)
public class HttpClientReplicaTests extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(HttpClientReplicaTests.class.getName());

    private TransportAddress httpAddress;

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        httpAddress = nodeInfo.getHttp().getAddress().publishAddress();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(Netty4Plugin.class);
    }

    @Override
    public Settings nodeSettings(int nodeNumber) {
        return Settings.builder()
                .put(super.nodeSettings(nodeNumber))
                .put(EsExecutors.PROCESSORS_SETTING.getKey(), 1)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_TYPE_DEFAULT_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    private String findHttpAddress() {
        return "http://" + httpAddress.address().getHostName() + ":" + httpAddress.address().getPort();
    }

    public void testReplicaLevel() throws Exception {

        Settings settingsTest1 = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 2)
                .build();

        Settings settingsTest2 = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .build();

        final HttpClient client = ClientBuilder.builder()
                .put("urls", findHttpAddress())
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);

        try {
            client.newIndex("test1", settingsTest1, null)
                    .newIndex("test2", settingsTest2, null);
            client.waitForCluster("GREEN", TimeValue.timeValueSeconds(30));
            for (int i = 0; i < 1234; i++) {
                client.index("test1", "test", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            for (int i = 0; i < 1234; i++) {
                client.index("test2", "test", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(60));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("refreshing");
            client.refreshIndex("test1");
            client.refreshIndex("test2");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test1", "test2")
                    .setQuery(QueryBuilders.matchAllQuery());
            long hits = searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
            logger.info("query total hits={}", hits);
            assertEquals(2468, hits);
            IndicesStatsRequestBuilder indicesStatsRequestBuilder = new IndicesStatsRequestBuilder(client.client(),
                    IndicesStatsAction.INSTANCE)
                    .all();
            IndicesStatsResponse response = indicesStatsRequestBuilder.execute().actionGet();
            for (Map.Entry<String, IndexStats> m : response.getIndices().entrySet()) {
                IndexStats indexStats = m.getValue();
                CommonStats commonStats = indexStats.getTotal();
                IndexingStats indexingStats = commonStats.getIndexing();
                IndexingStats.Stats stats = indexingStats.getTotal();
                logger.info("index {}: count = {}", m.getKey(), stats.getIndexCount());
                for (Map.Entry<Integer, IndexShardStats> me : indexStats.getIndexShards().entrySet()) {
                    IndexShardStats indexShardStats = me.getValue();
                    CommonStats commonShardStats = indexShardStats.getTotal();
                    logger.info("shard {} count = {}", me.getKey(),
                            commonShardStats.getIndexing().getTotal().getIndexCount());
                }
            }
            try {
                client.deleteIndex("test1")
                        .deleteIndex("test2");
            } catch (Exception e) {
                logger.error("delete index failed, ignored. Reason:", e);
            }
            client.shutdown();
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }

}
