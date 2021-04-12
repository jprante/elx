package org.xbib.elx.transport.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.transport.TransportAdminClient;
import org.xbib.elx.transport.TransportAdminClientProvider;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;

import java.util.concurrent.TimeUnit;

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
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getTransportSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                     .setBulkClientProvider(TransportBulkClientProvider.class)
                     .put(helper.getTransportSettings())
                     .build()) {
            IndexDefinition indexDefinition =
                    adminClient.buildIndexDefinitionFromSettings("test_smoke", Settings.EMPTY);
            assertEquals(0, indexDefinition.getReplicaLevel());
            assertEquals(helper.getClusterName(), adminClient.getClusterName());
            indexDefinition.setFullIndexName("test_smoke");
            indexDefinition.setType("doc");
            bulkClient.newIndex(indexDefinition);
            bulkClient.index("test_smoke", "doc", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.checkMapping("test_smoke");
            bulkClient.update("test_smoke", "doc", "1", "{ \"name\" : \"Another name\"}");
            bulkClient.delete("test_smoke", "doc", "1");
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            bulkClient.index("test_smoke", "doc", "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.delete("test_smoke", "doc", "1");
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.deleteIndex("test_smoke");
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition.getFullIndexName(), "doc", "1", true, "{ \"name\" : \"Hello World\"}");
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
        }
    }
}
