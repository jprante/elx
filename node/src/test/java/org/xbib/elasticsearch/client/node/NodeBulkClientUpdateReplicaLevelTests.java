package org.xbib.elasticsearch.client.node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.testframework.ESIntegTestCase;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=3)
public class NodeBulkClientUpdateReplicaLevelTests extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(NodeBulkClientUpdateReplicaLevelTests.class.getName());

    public void testUpdateReplicaLevel() throws Exception {

        int numberOfShards = 1;
        int replicaLevel = 2;

        int shardsAfterReplica;

        Settings settings = Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .build();

        final NodeBulkClient client = ClientBuilder.builder()
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(client(), NodeBulkClient.class);

        try {
            client.newIndex("replicatest", settings, null);
            client.waitForCluster("GREEN", "30s");
            for (int i = 0; i < 12345; i++) {
                client.index("replicatest", "replicatest", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
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
