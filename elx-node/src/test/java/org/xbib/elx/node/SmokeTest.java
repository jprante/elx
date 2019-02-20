package org.xbib.elx.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.api.IndexDefinition;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SmokeTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(SmokeTest.class.getSimpleName());

    @Test
    public void smokeTest() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .build();
        try {
            client.newIndex("test");
            client.index("test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flush();
            client.waitForResponses(30, TimeUnit.SECONDS);

            assertEquals(clusterName, client.getClusterName());

            client.checkMapping("test");

            client.update("test", "1", "{ \"name\" : \"Another name\"}");
            client.flush();

            client.waitForRecovery("test", 10L, TimeUnit.SECONDS);

            client.delete("test", "1");
            client.deleteIndex("test");

            IndexDefinition indexDefinition = client.buildIndexDefinitionFromSettings("test2", Settings.settingsBuilder()
                    .build());
            assertEquals(0, indexDefinition.getReplicaLevel());
            client.newIndex(indexDefinition);
            client.index(indexDefinition.getFullIndexName(), "1", true, "{ \"name\" : \"Hello World\"}");
            client.flush();
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
