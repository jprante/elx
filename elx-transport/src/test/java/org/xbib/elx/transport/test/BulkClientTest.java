package org.xbib.elx.transport.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.transport.TransportAdminClient;
import org.xbib.elx.transport.TransportAdminClientProvider;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;

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
        try (TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getTransportSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(30))
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setFullIndexName("test");
            indexDefinition.setType("doc");
            bulkClient.newIndex(indexDefinition);
            bulkClient.index("test", "docd", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
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
        try (TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getTransportSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setFullIndexName("test");
            indexDefinition.setType("doc");
            bulkClient.newIndex(indexDefinition);
        }
    }

    @Test
    void testMapping() throws Exception {
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getTransportSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                 .setBulkClientProvider(TransportBulkClientProvider.class)
                 .put(helper.getTransportSettings())
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
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setFullIndexName("test");
            indexDefinition.setType("doc");
            indexDefinition.setMappings(builder.string());
            bulkClient.newIndex(indexDefinition);
            assertTrue(adminClient.getMapping("test").containsKey("properties"));
        }
    }

    @Test
    void testRandomDocs() throws Exception {
        long numactions = ACTIONS;
        try (TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getTransportSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setFullIndexName("test");
            indexDefinition.setType("doc");
            bulkClient.newIndex(indexDefinition);
            for (int i = 0; i < ACTIONS; i++) {
                bulkClient.index("test", "docs", null, false,
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
    void testThreadedRandomDocs() {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        Long maxActionsPerRequest = MAX_ACTIONS_PER_REQUEST;
        final long actions = ACTIONS;
        logger.info("maxthreads={} maxactions={} maxloop={}", maxthreads, maxActionsPerRequest, actions);
        try (TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getTransportSettings())
                .put(Parameters.MAX_CONCURRENT_REQUESTS.name(), maxthreads * 2)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), maxActionsPerRequest)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .put(Parameters.ENABLE_BULK_LOGGING.name(), "true")
                .build()) {
            XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("index")
                    .field("number_of_shards", 1)
                    .field("number_of_replicas", 0)
                    .endObject()
                    .endObject();
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setIndex("test");
            indexDefinition.setType("doc");
            indexDefinition.setFullIndexName("test");
            indexDefinition.setSettings(builder.string());
            bulkClient.newIndex(indexDefinition);
            bulkClient.startBulk("test", 0, 1000);
            logger.info("index created");
            ExecutorService executorService = Executors.newFixedThreadPool(maxthreads);
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                executorService.execute(() -> {
                    for (int i1 = 0; i1 < actions; i1++) {
                        bulkClient.index("test", "docs", null, false,
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
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
