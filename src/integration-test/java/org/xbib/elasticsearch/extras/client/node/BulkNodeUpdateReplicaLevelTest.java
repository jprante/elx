package org.xbib.elasticsearch.extras.client.node;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;
import org.xbib.elasticsearch.extras.client.Clients;
import org.xbib.elasticsearch.extras.client.SimpleBulkControl;
import org.xbib.elasticsearch.extras.client.SimpleBulkMetric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 */
public class BulkNodeUpdateReplicaLevelTest extends NodeTestUtils {

    private static final ESLogger logger = ESLoggerFactory.getLogger(BulkNodeUpdateReplicaLevelTest.class.getSimpleName());

    @Test
    public void testUpdateReplicaLevel() throws Exception {

        int numberOfShards = 2;
        int replicaLevel = 3;

        // we need 3 nodes for replica level 3
        startNode("2");
        startNode("3");

        int shardsAfterReplica;

        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .build();

        final BulkNodeClient client = Clients.builder()
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));

        try {
            client.newIndex("replicatest", settings, null);
            client.waitForCluster("GREEN", "30s");
            for (int i = 0; i < 12345; i++) {
                client.index("replicatest", "replicatest", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
            shardsAfterReplica = client.updateReplicaLevel("replicatest", replicaLevel);
            assertEquals(shardsAfterReplica, numberOfShards * (replicaLevel + 1));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }

}
