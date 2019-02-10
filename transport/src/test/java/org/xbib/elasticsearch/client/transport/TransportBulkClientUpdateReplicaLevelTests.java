package org.xbib.elasticsearch.client.transport;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=3)
public class TransportBulkClientUpdateReplicaLevelTests extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(TransportBulkClientUpdateReplicaLevelTests.class.getName());

    private String clusterName;

    private TransportAddress address;

    @Before
    public void fetchClusterInfo() {
        clusterName = client().admin().cluster().prepareClusterStats().get().getClusterName().value();
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        address = nodeInfo.getTransport().getAddress().publishAddress();
    }

    private Settings ourTransportClientSettings() {
        return Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName)
                .put("host", address.address().getHostString() + ":" + address.getPort())
                .put(EsExecutors.PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
                .build();
    }

    public void testUpdateReplicaLevel() throws Exception {

        //ensureStableCluster(3);

        int shardsAfterReplica;

        Settings settings = Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .build();

        final TransportBulkClient client = ClientBuilder.builder()
                .put(ourTransportClientSettings())
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(TransportBulkClient.class);

        try {
            client.newIndex("replicatest", settings, null);
            client.waitForCluster("GREEN", TimeValue.timeValueSeconds(30));
            for (int i = 0; i < 12345; i++) {
                client.index("replicatest", "replicatest", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            shardsAfterReplica = client.updateReplicaLevel("replicatest", 3);
            assertEquals(shardsAfterReplica, 2 * (3 + 1));
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
