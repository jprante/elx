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
        final TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getTransportSettings())
                .build();
        final TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getTransportSettings())
                .build();
        IndexDefinition indexDefinition =
                adminClient.buildIndexDefinitionFromSettings("test_smoke", Settings.EMPTY);
        try {
            assertEquals(helper.getClusterName(), client.getClusterName());
            client.newIndex("test_smoke");
            client.index("test_smoke", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flush();
            client.waitForResponses(30, TimeUnit.SECONDS);
            client.checkMapping("test_smoke");
            client.update("test_smoke", "1", "{ \"name\" : \"Another name\"}");
            client.delete("test_smoke", "1");
            client.flush();
            client.waitForResponses(30, TimeUnit.SECONDS);
            client.checkMapping("test_smoke");
            client.deleteIndex("test_smoke");
            IndexDefinition indexDefinition = client.buildIndexDefinitionFromSettings("test_smoke", Settings.builder()
                    .build());
            assertEquals(0, indexDefinition.getReplicaLevel());
            client.newIndex(indexDefinition);
            client.index(indexDefinition.getFullIndexName(), "1", true, "{ \"name\" : \"Hello World\"}");
            client.flush();
            client.waitForResponses(30, TimeUnit.SECONDS);
            client.updateReplicaLevel(indexDefinition, 2);
            int replica = client.getReplicaLevel(indexDefinition);
            assertEquals(2, replica);
            client.deleteIndex(indexDefinition);
            assertEquals(0, client.getBulkMetric().getFailed().getCount());
            assertEquals(4, client.getBulkMetric().getSucceeded().getCount());
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            bulkClient.close();
            assertEquals(0, bulkClient.getBulkMetric().getFailed().getCount());
            assertEquals(4, bulkClient.getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
            // close admin after bulk
            adminClient.deleteIndex(indexDefinition);
            adminClient.close();
        }
    }
}
