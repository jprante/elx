package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.node.NodeAdminClient;
import org.xbib.elx.node.NodeAdminClientProvider;
import org.xbib.elx.node.NodeBulkClient;
import org.xbib.elx.node.NodeBulkClientProvider;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(TestExtension.class)
class SmokeTest {

    private static final Logger logger = LogManager.getLogger(SmokeTest.class.getName());

    private final TestExtension.Helper helper;

    SmokeTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void smokeTest() throws Exception {
        try (NodeAdminClient adminClient = ClientBuilder.builder(helper.client)
                .setAdminClientProvider(NodeAdminClientProvider.class)
                .put(helper.getNodeSettings())
                .build();
             NodeBulkClient bulkClient = ClientBuilder.builder(helper.client)
                .setBulkClientProvider(NodeBulkClientProvider.class)
                     .put(helper.getNodeSettings())
                .build()) {
            assertEquals(helper.getClusterName(), adminClient.getClusterName());
            IndexDefinition indexDefinition =
                    new DefaultIndexDefinition(adminClient, "test_smoke", "doc", Settings.EMPTY);
            assertEquals("test_smoke", indexDefinition.getIndex());
            assertTrue(indexDefinition.getFullIndexName().startsWith("test_smoke"));
            assertEquals(0, indexDefinition.getReplicaLevel());
            indexDefinition.setType("doc");
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.checkMapping(indexDefinition);
            bulkClient.update(indexDefinition, "1", "{ \"name\" : \"Another name\"}");
            bulkClient.delete(indexDefinition, "1");
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.delete(indexDefinition, "1");
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.deleteIndex(indexDefinition);
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.updateReplicaLevel(indexDefinition, 2);
            int replica = adminClient.getReplicaLevel(indexDefinition);
            assertEquals(2, replica);
            assertEquals(0, bulkClient.getBulkController().getBulkMetric().getFailed().getCount());
            assertEquals(6, bulkClient.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
            adminClient.deleteIndex(indexDefinition);
            logger.info("done");
        }
    }
}
