package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.node.ExtendedNodeClient;
import org.xbib.elx.node.ExtendedNodeClientProvider;

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
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .build();
        try {
            assertEquals(helper.getClusterName(), client.getClusterName());
            client.newIndex("test");
            client.index("test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.update("test", "1", "{ \"name\" : \"Another name\"}");
            client.delete("test", "1");
            client.flush();
            client.waitForResponses(30, TimeUnit.SECONDS);
            client.waitForRecovery("test", 10L, TimeUnit.SECONDS);
            client.delete("test", "1");
            client.flush();
            client.checkMapping("test");
            client.deleteIndex("test");
            IndexDefinition indexDefinition = client.buildIndexDefinitionFromSettings("test", Settings.builder()
                    .build());
            assertEquals(0, indexDefinition.getReplicaLevel());
            client.newIndex(indexDefinition);
            client.waitForRecovery(indexDefinition.getFullIndexName(), 30L, TimeUnit.SECONDS);
            client.index(indexDefinition.getFullIndexName(), "1", true, "{ \"name\" : \"Hello World\"}");
            client.flush();
            client.waitForResponses(30, TimeUnit.SECONDS);
            client.updateReplicaLevel(indexDefinition, 2);
            int replica = client.getReplicaLevel(indexDefinition);
            assertEquals(2, replica);
            client.deleteIndex(indexDefinition);
            assertEquals(0, client.getBulkMetric().getFailed().getCount());
            assertEquals(5, client.getBulkMetric().getSucceeded().getCount());
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
