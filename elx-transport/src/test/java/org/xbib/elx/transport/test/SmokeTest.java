package org.xbib.elx.transport.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.transport.ExtendedTransportClient;
import org.xbib.elx.transport.ExtendedTransportClientProvider;

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
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(helper.getTransportSettings())
                .build();
        try {
            assertEquals(helper.getClusterName(), client.getClusterName());
            client.newIndex("test_smoke");
            client.index("test_smoke", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
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
            client.close();
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
