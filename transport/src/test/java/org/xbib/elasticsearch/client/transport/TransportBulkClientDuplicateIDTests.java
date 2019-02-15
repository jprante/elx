package org.xbib.elasticsearch.client.transport;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.testframework.ESSingleNodeTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import java.util.Collection;
import java.util.Collections;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
public class TransportBulkClientDuplicateIDTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(TransportBulkClientDuplicateIDTests.class.getName());

    private static final long MAX_ACTIONS = 100L;

    private static final long NUM_ACTIONS = 12345L;

    private TransportAddress address;

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        address = nodeInfo.getTransport().getAddress().publishAddress();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(Netty4Plugin.class);
    }

    @Override
    public Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "test-cluster")
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                .build();
    }

    private Settings transportClientSettings() {
        return Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "test-cluster")
                .put("host", address.address().getHostString() + ":" + address.getPort())
                .put(EsExecutors.PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
                .build();
    }

    public void testDuplicateDocIDs() throws Exception {
        final TransportBulkClient client = ClientBuilder.builder()
                .put(transportClientSettings())
                .put(ClientBuilder.MAX_CONCURRENT_REQUESTS, 2) // avoid EsRejectedExecutionException
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(TransportBulkClient.class);
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", randomAlphaOfLength(1), false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setTypes("test")
                    .setQuery(matchAllQuery());
            long hits = searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
            logger.info("hits = {}", hits);
            assertTrue(hits < NUM_ACTIONS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            logger.info("numactions = {}, submitted = {}, succeeded= {}, failed = {}", NUM_ACTIONS,
                    client.getMetric().getSubmitted().getCount(),
                    client.getMetric().getSucceeded().getCount(),
                    client.getMetric().getFailed().getCount());
            assertEquals(NUM_ACTIONS, client.getMetric().getSubmitted().getCount());
            assertEquals(NUM_ACTIONS, client.getMetric().getSucceeded().getCount());
        }
    }
}
