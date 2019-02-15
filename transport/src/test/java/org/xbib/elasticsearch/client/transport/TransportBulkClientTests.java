package org.xbib.elasticsearch.client.transport;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.testframework.ESSingleNodeTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
public class TransportBulkClientTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(TransportBulkClientTests.class.getName());

    private static final Long MAX_ACTIONS = 10L;

    private static final Long NUM_ACTIONS = 1234L;

    private TransportAddress address;

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

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        address = nodeInfo.getTransport().getAddress().publishAddress();
    }

    public void testBulkTransportClientNewIndex() throws Exception {
        logger.info("firing up BulkTransportClient");
        final TransportBulkClient client = ClientBuilder.builder()
                .put(transportClientSettings())
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(TransportBulkClient.class);
        try {
            logger.info("creating index");
            client.newIndex("test");
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            logger.info("deleting/creating index: start");
            client.deleteIndex("test")
                    .newIndex("test")
                    .deleteIndex("test");
            logger.info("deleting/creating index: end");
        } catch (NoNodeAvailableException e) {
            logger.error("no node available");
        } finally {
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    public void testBulkTransportClientMapping() throws Exception {
        final TransportBulkClient client = ClientBuilder.builder()
                .put(transportClientSettings())
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(TransportBulkClient.class);
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
        client.mapping("test", Strings.toString(builder));
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

    public void testBulkTransportClientSingleDoc() throws IOException {
        logger.info("firing up BulkTransportClient");
        final TransportBulkClient client = ClientBuilder.builder()
                .put(transportClientSettings())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(TransportBulkClient.class);
        try {
            logger.info("creating index");
            client.newIndex("test");
            logger.info("indexing one doc");
            client.index("test", "test", "1", false, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            logger.info("flush");
            client.flushIngest();
            logger.info("wait for responses");
            client.waitForResponses("30s");
            logger.info("waited for responses");
        } catch (InterruptedException e) {
            // ignore
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(1, client.getMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    public void testBulkTransportClientRandomDocs() throws Exception {
        long numactions = NUM_ACTIONS;
        final TransportBulkClient client = ClientBuilder.builder()
                .put(transportClientSettings())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(TransportBulkClient.class);
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", null, false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
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

    public void testBulkTransportClientThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        long maxactions = MAX_ACTIONS;
        final long maxloop = NUM_ACTIONS;
        logger.info("TransportClient max={} maxactions={} maxloop={}", maxthreads, maxactions, maxloop);
        final TransportBulkClient client = ClientBuilder.builder()
                .put(transportClientSettings())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, maxactions)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60)) // = effectively disables autoflush for this test
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(TransportBulkClient.class);
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
            logger.info("client flush ...");
            client.flushIngest();
            client.waitForResponses("30s");
            logger.info("executor service to be shut down ...");
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
    }
}
