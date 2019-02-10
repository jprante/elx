package org.xbib.elasticsearch.client.http;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import java.util.Collection;
import java.util.Collections;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
public class HttpClientDuplicateIDTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(HttpClientDuplicateIDTests.class.getName());

    private static final long MAX_ACTIONS = 10L;

    private static final long NUM_ACTIONS = 12345L;

    private TransportAddress httpAddress;

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        httpAddress = nodeInfo.getHttp().getAddress().publishAddress();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(Netty4Plugin.class);
    }

    @Override
    public Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_TYPE_DEFAULT_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    private String findHttpAddress() {
        return "http://" + httpAddress.address().getHostName() + ":" + httpAddress.address().getPort();
    }

    public void testDuplicateDocIDs() throws Exception {
        final HttpClient client = ClientBuilder.builder()
                //.put(ClientBuilder.MAX_CONCURRENT_REQUESTS, 2) // avoid EsRejectedExecutionException
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put("urls", findHttpAddress())
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", randomAlphaOfLength(1), false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setTypes("test")
                    .setQuery(QueryBuilders.matchAllQuery());
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
