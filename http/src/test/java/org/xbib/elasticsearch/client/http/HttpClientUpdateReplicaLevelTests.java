package org.xbib.elasticsearch.client.http;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import java.util.Collection;
import java.util.Collections;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=3)
public class HttpClientUpdateReplicaLevelTests extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(HttpClientUpdateReplicaLevelTests.class.getName());

    private TransportAddress httpAddress;

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        httpAddress = nodeInfo.getHttp().getAddress().publishAddress();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(Netty4Plugin.class);
    }

    @Override
    public Settings nodeSettings(int nodeNumber) {
        return Settings.builder()
                .put(super.nodeSettings(nodeNumber))
                .put(EsExecutors.PROCESSORS_SETTING.getKey(), 1)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_TYPE_DEFAULT_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    private String findHttpAddress() {
        return "http://" + httpAddress.address().getHostName() + ":" + httpAddress.address().getPort();
    }

    public void testUpdateReplicaLevel() throws Exception {

        int numberOfShards = 1;
        int replicaLevel = 2;

        int shardsAfterReplica;

        Settings settings = Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", 0)
                .build();

        final HttpClient client = ClientBuilder.builder()
                .put("urls", findHttpAddress())
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(client(), HttpClient.class);

        try {
            client.newIndex("replicatest", settings, null);
            client.waitForCluster("GREEN", TimeValue.timeValueSeconds(30));
            for (int i = 0; i < 12345; i++) {
                client.index("replicatest", "replicatest", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
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
