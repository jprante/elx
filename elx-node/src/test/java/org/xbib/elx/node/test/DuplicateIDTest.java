package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.node.NodeBulkClient;
import org.xbib.elx.node.NodeBulkClientProvider;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class DuplicateIDTest {

    private static final Logger logger = LogManager.getLogger(DuplicateIDTest.class.getName());

    private static final Long ACTIONS = 100L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 5L;

    private final TestExtension.Helper helper;

    DuplicateIDTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testDuplicateDocIDs() throws Exception {
        long numactions = ACTIONS;
        try (NodeBulkClient bulkClient = ClientBuilder.builder(helper.client())
                .setBulkClientProvider(NodeBulkClientProvider.class)
                .put(helper.getNodeSettings())
                .put(Parameters.BULK_MAX_ACTIONS_PER_REQUEST.getName(), MAX_ACTIONS_PER_REQUEST)
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
            assertEquals(numactions, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }
}
