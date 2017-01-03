package org.xbib.elasticsearch.extras.client.node;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;
import org.xbib.elasticsearch.extras.client.Clients;
import org.xbib.elasticsearch.extras.client.SimpleBulkControl;
import org.xbib.elasticsearch.extras.client.SimpleBulkMetric;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BulkNodeClientTest extends NodeTestUtils {

    private static final ESLogger logger = ESLoggerFactory.getLogger(BulkNodeClientTest.class.getSimpleName());

    private static final Long MAX_ACTIONS = 1000L;

    private static final Long NUM_ACTIONS = 1234L;

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
    public void testNewIndexNodeClient() throws Exception {
        final BulkNodeClient client = Clients.builder()
                .put(Clients.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
        client.newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testMappingNodeClient() throws Exception {
        final BulkNodeClient client = Clients.builder()
                .put(Clients.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
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
                client.client().execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
        logger.info("mappings={}", getMappingsResponse.getMappings());
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testSingleDocNodeClient() {
        final BulkNodeClient client = Clients.builder()
                .put(Clients.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(Clients.FLUSH_INTERVAL, TimeValue.timeValueSeconds(30))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
        try {
            client.newIndex("test");
            client.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses("30s");
        } catch (InterruptedException e) {
            // ignore
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } finally {
            assertEquals(1, client.getMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testRandomDocsNodeClient() throws Exception {
        long numactions = NUM_ACTIONS;
        final BulkNodeClient client = Clients.builder()
                .put(Clients.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(Clients.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(numactions, client.getMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocsNodeClient() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        Long maxactions = MAX_ACTIONS;
        final Long maxloop = NUM_ACTIONS;
        logger.info("NodeClient max={} maxactions={} maxloop={}", maxthreads, maxactions, maxloop);
        final BulkNodeClient client = Clients.builder()
                .put(Clients.MAX_ACTIONS_PER_REQUEST, maxactions)
                .put(Clients.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))// disable auto flush for this test
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
        try {
            client.newIndex("test")
                    .startBulk("test", -1, 1000);
            ThreadPoolExecutor pool = EsExecutors.newFixed("bulk-nodeclient-test", maxthreads, 30,
                    EsExecutors.daemonThreadFactory("bulk-nodeclient-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < maxloop; i++) {
                            client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 30 seconds...");
            latch.await(30, TimeUnit.SECONDS);
            logger.info("flush...");
            client.flushIngest();
            client.waitForResponses("30s");
            logger.info("got all responses, thread pool shutdown...");
            pool.shutdown();
            logger.info("pool is shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            assertEquals(maxthreads * maxloop, client.getMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(maxthreads * maxloop,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.shutdown();
        }
    }

}
