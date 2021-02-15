package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.node.NodeAdminClient;
import org.xbib.elx.node.NodeAdminClientProvider;
import org.xbib.elx.node.NodeBulkClient;
import org.xbib.elx.node.NodeBulkClientProvider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class BulkClientTest {

    private static final Logger logger = LogManager.getLogger(BulkClientTest.class.getName());

    private static final Long ACTIONS = 10000L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 10000L;

    private final TestExtension.Helper helper;

    BulkClientTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testSingleDoc() throws Exception {
        try (NodeBulkClient bulkClient = ClientBuilder.builder(helper.client)
                .setBulkClientProvider(NodeBulkClientProvider.class)
                .put(helper.getNodeSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(30))
                .build()) {
            bulkClient.newIndex("test");
            bulkClient.index("test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            assertEquals(1, bulkClient.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
        }
    }

    @Test
    void testNewIndex() throws Exception {
        try (NodeBulkClient bulkClient = ClientBuilder.builder(helper.client)
                .setBulkClientProvider(NodeBulkClientProvider.class)
                .put(helper.getNodeSettings())
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build()) {
            bulkClient.newIndex("test");
        }
    }

    @Test
    void testMapping() throws Exception {
        try (NodeAdminClient adminClient = ClientBuilder.builder(helper.client)
                .setAdminClientProvider(NodeAdminClientProvider.class)
                .put(helper.getNodeSettings())
                .build();
             NodeBulkClient bulkClient = ClientBuilder.builder(helper.client)
                     .setBulkClientProvider(NodeBulkClientProvider.class)
                     .put(helper.getNodeSettings())
                     .build()) {
            XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("doc")
                    .startObject("properties")
                    .startObject("location")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            bulkClient.newIndex("test", Settings.EMPTY, builder);
            assertTrue(adminClient.getMapping("test").containsKey("properties"));
        }
    }

    @Test
    void testRandomDocs() throws Exception {
        long numactions = ACTIONS;
        try (NodeBulkClient bulkClient = ClientBuilder.builder(helper.client)
                .setBulkClientProvider(NodeBulkClientProvider.class)
                .put(helper.getNodeSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build()) {
            bulkClient.newIndex("test");
            for (int i = 0; i < ACTIONS; i++) {
                bulkClient.index("test", null, false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            assertEquals(numactions, bulkClient.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
            bulkClient.refreshIndex("test");
            assertEquals(numactions, bulkClient.getSearchableDocs("test"));
        }
    }

    @Test
    void testThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        Long maxActionsPerRequest = MAX_ACTIONS_PER_REQUEST;
        final long actions = ACTIONS;
        logger.info("maxthreads={} maxactions={} maxloop={}", maxthreads, maxActionsPerRequest, actions);
        try (NodeBulkClient bulkClient = ClientBuilder.builder(helper.client)
                .setBulkClientProvider(NodeBulkClientProvider.class)
                .put(helper.getNodeSettings())
                .put(Parameters.MAX_CONCURRENT_REQUESTS.name(), maxthreads)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), maxActionsPerRequest)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build()) {
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            bulkClient.newIndex("test", settings);
            bulkClient.startBulk("test", 0, 1000);
            ExecutorService executorService = Executors.newFixedThreadPool(maxthreads);
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                executorService.execute(() -> {
                    for (int i1 = 0; i1 < actions; i1++) {
                        bulkClient.index("test", null, false,
                                "{ \"name\" : \"" + helper.randomString(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            logger.info("waiting for latch...");
            if (latch.await(30L, TimeUnit.SECONDS)) {
                logger.info("flush...");
                bulkClient.flush();
                bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
                logger.info("got all responses, executor service shutdown...");
                executorService.shutdown();
                executorService.awaitTermination(30L, TimeUnit.SECONDS);
                logger.info("pool is shut down");
            } else {
                logger.warn("latch timeout");
            }
            bulkClient.stopBulk("test", 30L, TimeUnit.SECONDS);
            assertEquals(maxthreads * actions, bulkClient.getBulkController().getBulkMetric().getSucceeded().getCount());
            bulkClient.refreshIndex("test");
            assertEquals(maxthreads * actions, bulkClient.getSearchableDocs("test"));
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
        }
    }
}
