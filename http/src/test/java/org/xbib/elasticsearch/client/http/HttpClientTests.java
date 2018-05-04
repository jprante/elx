package org.xbib.elasticsearch.client.http;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
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
public class HttpClientTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(HttpClientTests.class.getName());

    private static final Long MAX_ACTIONS = 10L;

    private static final Long NUM_ACTIONS = 1234L;

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

    public void testNewIndex() throws Exception {
        final HttpClient client = ClientBuilder.builder()
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
                .put("urls", findHttpAddress())
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        client.newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    /*public void testMapping() throws Exception {
        final HttpClient client = ClientBuilder.builder()
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
                .put("urls", findHttpAddress())
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("test")
                .startObject("properties")
                .startObject("location")
                .field("type", "geo_point")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client.mapping("test", builder.string());
        client.newIndex("test");
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices("test");
        GetMappingsResponse getMappingsResponse =
                client.client().execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
        logger.info("mappings={}", getMappingsResponse.getMappings());
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    public void testSingleDoc() throws Exception {
        final HttpClient client = ClientBuilder.builder()
                .put("urls", findHttpAddress())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(30))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        client.newIndex("test");
        client.index("test", "test", "1", false,"{ \"name\" : \"Hello World\"}"); // single doc ingest
        client.flushIngest();
        client.waitForResponses(TimeValue.timeValueSeconds(30));
        assertEquals(1, client.getMetric().getSucceeded().getCount());
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    public void testRandomDocs() throws Exception {
        long numactions = NUM_ACTIONS;
        final HttpClient client = ClientBuilder.builder()
                .put("urls", findHttpAddress())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            logger.info("assuring {} == {}", numactions, client.getMetric().getSucceeded().getCount());
            assertEquals(numactions, client.getMetric().getSucceeded().getCount());
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    public void testThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        Long maxactions = MAX_ACTIONS;
        final Long maxloop = NUM_ACTIONS;
        logger.info("max={} maxactions={} maxloop={}", maxthreads, maxactions, maxloop);
        final HttpClient client = ClientBuilder.builder()
                .put("urls", findHttpAddress())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, maxactions)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))// disable auto flush for this test
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        try {
            client.newIndex("test").startBulk("test", 30 * 1000, 1000);
            ExecutorService executorService = Executors.newFixedThreadPool(maxthreads);
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                executorService.execute(() -> {
                    for (int i1 = 0; i1 < maxloop; i1++) {
                        client.index("test", "test", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            logger.info("waiting for max 30 seconds...");
            latch.await(30, TimeUnit.SECONDS);
            logger.info("flush...");
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            logger.info("got all responses, executor service shutdown...");
            executorService.shutdown();
            logger.info("executor service is shut down");
            client.stopBulk("test");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("assuring {} == {}", maxthreads * maxloop, client.getMetric().getSucceeded().getCount());
            assertEquals(maxthreads * maxloop, client.getMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0);
            assertEquals(maxthreads * maxloop,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.shutdown();
        }
    }*/
}
