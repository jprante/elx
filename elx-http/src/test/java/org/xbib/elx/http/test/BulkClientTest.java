package org.xbib.elx.http.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.http.HttpBulkClient;
import org.xbib.elx.http.HttpBulkClientProvider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(TestExtension.class)
class BulkClientTest {

    private static final Logger logger = LogManager.getLogger(BulkClientTest.class.getSimpleName());

    private static final Long ACTIONS = 1000L;

    private final TestExtension.Helper helper;

    BulkClientTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testNewIndex() throws Exception {
        try (HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            bulkClient.newIndex(indexDefinition);
        }
    }

    @Test
    void testSingleDoc() throws Exception {
        try (HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            if (bulkClient.getBulkProcessor().isBulkMetricEnabled()) {
                assertEquals(1, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            }
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }

    @Test
    void testRandomDocs() throws Exception {
        long numactions = ACTIONS;
        try (HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            bulkClient.newIndex(indexDefinition);
            bulkClient.startBulk(indexDefinition);
            for (int i = 0; i < ACTIONS; i++) {
                bulkClient.index(indexDefinition, null, false, "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            if (bulkClient.getBulkProcessor().isBulkMetricEnabled()) {
                assertEquals(numactions, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            }
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
            bulkClient.refreshIndex(indexDefinition);
            assertEquals(numactions, bulkClient.getSearchableDocs(indexDefinition));
        }
    }

    @Test
    void testThreadedRandomDocs() throws Exception {
        final int maxthreads = Runtime.getRuntime().availableProcessors();
        final long actions = ACTIONS;
        final long timeout = 120L;
        try (HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            bulkClient.newIndex(indexDefinition);
            bulkClient.startBulk(indexDefinition);
            ExecutorService executorService = Executors.newFixedThreadPool(maxthreads);
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                executorService.execute(() -> {
                    for (int i1 = 0; i1 < actions; i1++) {
                        bulkClient.index(indexDefinition, null, false,
                                "{ \"name\" : \"" + helper.randomString(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            if (latch.await(timeout, TimeUnit.SECONDS)) {
                bulkClient.waitForResponses(timeout, TimeUnit.SECONDS);
                executorService.shutdown();
                executorService.awaitTermination(timeout, TimeUnit.SECONDS);
            } else {
                logger.error("latch timeout!");
            }
            bulkClient.stopBulk(indexDefinition);
            bulkClient.refreshIndex(indexDefinition);
            assertEquals(maxthreads * actions, bulkClient.getSearchableDocs(indexDefinition));
            if (bulkClient.getBulkProcessor().isBulkMetricEnabled()) {
                assertEquals(maxthreads * actions, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            }
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }
}
