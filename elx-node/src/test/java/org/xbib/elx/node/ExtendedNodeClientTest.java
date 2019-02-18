package org.xbib.elx.node;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExtendedNodeClientTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ExtendedNodeClientTest.class.getSimpleName());

    private static final Long ACTIONS = 25000L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 1000L;

    @Before
    public void startNodes() {
        try {
            super.startNodes();
            startNode("2");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    @Test
    public void testSingleDocNodeClient() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(30))
                .build();
        try {
            client.newIndex("test");
            client.index("test", "test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses("30s");
        } catch (InterruptedException e) {
            // ignore
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(1, client.getBulkMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testNewIndexNodeClient() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        client.newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testMappingNodeClient() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        XContentBuilder builder = jsonBuilder()
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
                client.getClient().execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
        logger.info("mappings={}", getMappingsResponse.getMappings());
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testRandomDocsNodeClient() throws Exception {
        long numactions = ACTIONS;
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test");
            for (int i = 0; i < ACTIONS; i++) {
                client.index("test", "test", null, false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(numactions, client.getBulkMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(numactions,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocsNodeClient() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        Long maxActionsPerRequest = MAX_ACTIONS_PER_REQUEST;
        final Long actions = ACTIONS;
        logger.info("NodeClient max={} maxactions={} maxloop={}", maxthreads, maxActionsPerRequest, actions);
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_CONCURRENT_REQUESTS.name(), maxthreads * 2)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), maxActionsPerRequest)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test")
                    .startBulk("test", -1, 1000);
            ThreadPoolExecutor pool = EsExecutors.newFixed("bulk-nodeclient-test", maxthreads, 30,
                    EsExecutors.daemonThreadFactory("bulk-nodeclient-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(() -> {
                    for (int i1 = 0; i1 < actions; i1++) {
                        client.index("test", "test", null, false,"{ \"name\" : \"" + randomString(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            logger.info("waiting for latch...");
            if (latch.await(5, TimeUnit.MINUTES)) {
                logger.info("last flush...");
                client.flushIngest();
                client.waitForResponses("60s");
                logger.info("got all responses, pool shutdown...");
                pool.shutdown();
                logger.info("pool is shut down");
            } else {
                logger.warn("latch timeout");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            assertEquals(maxthreads * actions, client.getBulkMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(maxthreads * actions,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.shutdown();
        }
    }
}
