package org.xbib.elasticsearch.extras.client.node;

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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestBase;
import org.xbib.elasticsearch.extras.client.ClientBuilder;
import org.xbib.elasticsearch.extras.client.SimpleBulkControl;
import org.xbib.elasticsearch.extras.client.SimpleBulkMetric;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BulkNodeClientTest extends NodeTestBase {

    private static final Logger logger = LogManager.getLogger(BulkNodeClientTest.class.getName());

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
        final BulkNodeClient client = ClientBuilder.builder()
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
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
    public void testBulkNodeClientMapping() throws Exception {
        final BulkNodeClient client = ClientBuilder.builder()
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
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
    public void testBulkNodeClientSingleDoc() {
        final BulkNodeClient client = ClientBuilder.builder()
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(30))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
        try {
            client.newIndex("test");
            client.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
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
    public void testBulkNodeClientRandomDocs() throws Exception {
        long numactions = NUM_ACTIONS;
        final BulkNodeClient client = ClientBuilder.builder()
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
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

    @Test
    public void testBulkNodeClientThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        Long maxactions = MAX_ACTIONS;
        final Long maxloop = NUM_ACTIONS;
        logger.info("NodeClient max={} maxactions={} maxloop={}", maxthreads, maxactions, maxloop);
        final BulkNodeClient client = ClientBuilder.builder()
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, maxactions)
                .put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(60))// disable auto flush for this test
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkNodeClient(client("1"));
        try {
            client.newIndex("test").startBulk("test", 30 * 1000, 1000);
            ExecutorService executorService = Executors.newFixedThreadPool(maxthreads);
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                executorService.execute(() -> {
                    for (int i1 = 0; i1 < maxloop; i1++) {
                        client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
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
    }
}
