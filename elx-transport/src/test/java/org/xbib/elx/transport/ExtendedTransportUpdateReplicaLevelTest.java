package org.xbib.elx.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExtendedTransportUpdateReplicaLevelTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ExtendedTransportUpdateReplicaLevelTest.class.getSimpleName());

    @Test
    public void testUpdateReplicaLevel() throws Exception {

        long numberOfShards = 2;
        int replicaLevel = 3;

        // we need 3 nodes for replica level 3
        startNode("2");
        startNode("3");

        int shardsAfterReplica;

        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
                .build();

        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .build();

        try {
            client.newIndex("replicatest", settings, new HashMap<>());
            client.waitForCluster("GREEN", "30s");
            for (int i = 0; i < 12345; i++) {
                client.index("replicatest", null, false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
            client.updateReplicaLevel("replicatest", replicaLevel, "30s");
            //assertEquals(shardsAfterReplica, numberOfShards * (replicaLevel + 1));
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
