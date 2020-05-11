package org.xbib.elx.node.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.node.ExtendedNodeClient;
import org.xbib.elx.node.ExtendedNodeClientProvider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class ClientTest {

    private static final Logger logger = LogManager.getLogger(ClientTest.class.getName());

    private static final Long ACTIONS = 10000L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 10000L;

    private final TestExtension.Helper helper;

    ClientTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testSingleDoc() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(30))
                .build();
        try {
            client.newIndex("test");
            client.index("test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(1, client.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.close();
        }
    }

    @Test
    void testNewIndex() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        client.newIndex("test");
        client.close();
    }

    @Test
    void testNewIndexWithSettings() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        Settings settings = Settings.builder().put("index.number_of_shards", "1").build();
        client.newIndex("test", settings);
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
                .indices("test");
        GetSettingsResponse getSettingsResponse =
                client.getClient().execute(GetSettingsAction.INSTANCE, getSettingsRequest).actionGet();
        logger.log(Level.INFO, "settings=" + getSettingsResponse.getSetting("test", "index.number_of_shards"));
        assertEquals("1", getSettingsResponse.getSetting("test", "index.number_of_shards"));
        client.close();
    }

    @Test
    void testNewIndexWithSettingsAndMappings() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        Settings settings = Settings.builder().put("index.number_of_shards", "1").build();
        XContentBuilder builder = JsonXContent.contentBuilder()
                .startObject()
                .field("date_detection", false)
                .startObject("properties")
                .startObject("location")
                .field("type", "geo_point")
                .endObject()
                .endObject()
                .endObject();
        client.newIndex("test", settings, builder);
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
                .indices("test");
        GetSettingsResponse getSettingsResponse =
                client.getClient().execute(GetSettingsAction.INSTANCE, getSettingsRequest).actionGet();
        logger.log(Level.INFO, "settings=" + getSettingsResponse.getSetting("test", "index.number_of_shards"));
        assertEquals("1", getSettingsResponse.getSetting("test", "index.number_of_shards"));
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest()
                .indices("test");
        GetMappingsResponse getMappingsResponse =
                client.getClient().execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
        logger.info("mappings={}", getMappingsResponse.getMappings());
        assertTrue(getMappingsResponse.getMappings().get("test").containsKey("_doc"));
        client.close();
    }

    @Test
    void testRandomDocs() throws Exception {
        long numactions = ACTIONS;
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test");
            for (int i = 0; i < ACTIONS; i++) {
                client.index("test", null, false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(numactions, client.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(numactions, searchRequestBuilder.execute().actionGet().getHits().getTotalHits().value);
            client.close();
        }
    }

    @Test
    void testThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        Long maxActionsPerRequest = MAX_ACTIONS_PER_REQUEST;
        final long actions = ACTIONS;
        logger.info("maxthreads={} maxactions={} maxloop={}", maxthreads, maxActionsPerRequest, actions);
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_CONCURRENT_REQUESTS.name(), maxthreads)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), maxActionsPerRequest)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            client.newIndex("test", settings, (String)null)
                    .startBulk("test", 0, 1000);
            logger.info("index created");
            ExecutorService executorService = Executors.newFixedThreadPool(maxthreads);
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                executorService.execute(() -> {
                    for (int i1 = 0; i1 < actions; i1++) {
                        client.index("test", null, false,
                                "{ \"name\" : \"" + helper.randomString(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            logger.info("waiting for latch...");
            if (latch.await(30L, TimeUnit.SECONDS)) {
                logger.info("flush...");
                client.flush();
                client.waitForResponses(30L, TimeUnit.SECONDS);
                logger.info("got all responses, executor service shutdown...");
                executorService.shutdown();
                executorService.awaitTermination(30L, TimeUnit.SECONDS);
                logger.info("pool is shut down");
            } else {
                logger.warn("latch timeout");
            }
            client.stopBulk("test", 30L, TimeUnit.SECONDS);
            assertEquals(maxthreads * actions, client.getBulkController().getBulkMetric().getSucceeded().getCount());
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            assertEquals(maxthreads * actions, client.getBulkController().getBulkMetric().getSucceeded().getCount());
            logger.log(Level.INFO, "refreshing index test");
            client.refreshIndex("test");
            SearchSourceBuilder builder = new SearchSourceBuilder()
                    .query(QueryBuilders.matchAllQuery())
                    .size(0)
                    .trackTotalHits(true);
            SearchRequest searchRequest = new SearchRequest()
                    .indices("test")
                    .source(builder);
            SearchResponse searchResponse = client.getClient().execute(SearchAction.INSTANCE, searchRequest).actionGet();
            assertEquals(maxthreads * actions, searchResponse.getHits().getTotalHits().value);
            client.close();
        }
    }
}
