package org.xbib.elx.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.indexing.IndexingStats;
import org.junit.Ignore;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore
public class ExtendedNodeReplicaTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ExtendedNodeReplicaTest.class.getSimpleName());

    @Test
    public void testReplicaLevel() throws Exception {

        // we need nodes for replica levels
        startNode("2");
        startNode("3");
        startNode("4");

        Settings settingsTest1 = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 3)
                .build();

        Settings settingsTest2 = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();

        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .build();

        try {
            client.newIndex("test1", settingsTest1, new HashMap<>())
                    .newIndex("test2", settingsTest2, new HashMap<>());
            client.waitForCluster("GREEN", "30s");
            for (int i = 0; i < 1234; i++) {
                client.index("test1", null, false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            for (int i = 0; i < 1234; i++) {
                client.index("test2", null, false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("refreshing");
            client.refreshIndex("test1");
            client.refreshIndex("test2");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setIndices("test1", "test2")
                    .setQuery(matchAllQuery());
            long hits = searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
            logger.info("query total hits={}", hits);
            assertEquals(2468, hits);
            IndicesStatsRequestBuilder indicesStatsRequestBuilder = new IndicesStatsRequestBuilder(client.getClient(), IndicesStatsAction.INSTANCE)
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
