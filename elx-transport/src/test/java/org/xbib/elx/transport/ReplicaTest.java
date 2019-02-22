package org.xbib.elx.transport;

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
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ReplicaTest extends TestBase {

    private static final Logger logger = LogManager.getLogger(ReplicaTest.class.getName());

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

        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getTransportSettings())
                .build();

        try {
            client.newIndex("test1", settingsTest1, new HashMap<>())
                    .newIndex("test2", settingsTest2, new HashMap<>());
            client.waitForCluster("GREEN", 30L, TimeUnit.SECONDS);
            for (int i = 0; i < 1234; i++) {
                client.index("test1", null, false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            for (int i = 0; i < 1234; i++) {
                client.index("test2", null, false,  "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
            client.refreshIndex("test1");
            client.refreshIndex("test2");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setIndices("test1", "test2")
                    .setQuery(matchAllQuery());
            long hits = searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
            logger.info("query total hits={}", hits);
            assertEquals(2468, hits);

            // TODO move to api
            IndicesStatsRequestBuilder indicesStatsRequestBuilder = new IndicesStatsRequestBuilder(client.getClient(),
                    IndicesStatsAction.INSTANCE).all();
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
                client.deleteIndex("test1").deleteIndex("test2");
            } catch (Exception e) {
                logger.error("delete index failed, ignored. Reason:", e);
            }
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.close();
        }
    }

    @Test
    public void testUpdateReplicaLevel() throws Exception {

        long numberOfShards = 2;
        int replicaLevel = 3;

        // we need 3 nodes for replica level 3
        startNode("2");
        startNode("3");

        int shardsAfterReplica;

        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getTransportSettings())
                .build();

        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .build();

        try {
            client.newIndex("replicatest", settings, new HashMap<>());
            client.waitForCluster("GREEN", 30L, TimeUnit.SECONDS);
            for (int i = 0; i < 12345; i++) {
                client.index("replicatest", null, false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
            client.updateReplicaLevel("replicatest", replicaLevel, 30L, TimeUnit.SECONDS);
            assertEquals(replicaLevel, client.getReplicaLevel("replicatest"));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.close();
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
