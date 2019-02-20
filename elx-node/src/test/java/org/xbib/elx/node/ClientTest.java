package org.xbib.elx.node;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
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

public class ClientTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ClientTest.class.getSimpleName());

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
    public void testSingleDoc() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(30))
                .build();
        try {
            client.newIndex("test");
            client.index("test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(1, client.getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.close();
        }
    }

    @Test
    public void testNewIndex() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        client.newIndex("test");
        client.close();
    }

    @Test
    public void testMapping() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject("doc")
                .startObject("properties")
                .startObject("location")
                .field("type", "geo_point")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client.newIndex("test", Settings.EMPTY, builder.string());
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices("test");
        GetMappingsResponse getMappingsResponse =
                client.getClient().execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
        logger.info("mappings={}", getMappingsResponse.getMappings());
        assertTrue(getMappingsResponse.getMappings().get("test").containsKey("doc"));
        client.close();
    }

    @Test
    public void testRandomDocs() throws Exception {
        long numactions = ACTIONS;
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test");
            for (int i = 0; i < ACTIONS; i++) {
                client.index("test", null, false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(numactions, client.getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(numactions,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.close();
        }
    }

    @Test
    public void testThreadedRandomDocs() throws Exception {
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
                        client.index("test", null, false,"{ \"name\" : \"" + randomString(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            logger.info("waiting for latch...");
            if (latch.await(5, TimeUnit.MINUTES)) {
                logger.info("flush...");
                client.flush();
                client.waitForResponses(60L, TimeUnit.SECONDS);
                logger.info("got all responses, pool shutdown...");
                pool.shutdown();
                logger.info("pool is shut down");
            } else {
                logger.warn("latch timeout");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test", 30L, TimeUnit.SECONDS);
            assertEquals(maxthreads * actions, client.getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(maxthreads * actions,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.close();
        }
    }
}
