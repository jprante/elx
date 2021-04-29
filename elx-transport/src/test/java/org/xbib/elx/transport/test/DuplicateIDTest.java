package org.xbib.elx.transport.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class DuplicateIDTest {

    private static final Logger logger = LogManager.getLogger(DuplicateIDTest.class.getName());

    private static final Long ACTIONS = 10000L;

    private final TestExtension.Helper helper;

    DuplicateIDTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testDuplicateDocIDs() throws Exception {
        long numactions = ACTIONS;
        try (TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            bulkClient.newIndex(indexDefinition);
            for (int i = 0; i < ACTIONS; i++) {
                bulkClient.index(indexDefinition, helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            bulkClient.refreshIndex(indexDefinition);
            assertTrue(bulkClient.getSearchableDocs(indexDefinition) < ACTIONS);
            if (bulkClient.getBulkProcessor().isBulkMetricEnabled()) {
                assertEquals(numactions, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            }
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }
}
