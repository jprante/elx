package org.xbib.elx.node.test;

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
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.node.ExtendedNodeClient;
import org.xbib.elx.node.ExtendedNodeClientProvider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class ClientTest {

    private static final Logger logger = LogManager.getLogger(ClientTest.class.getName());

    private static final Long ACTIONS = 1000L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 100L;

    private final TestExtension.Helper helper;

    ClientTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testNewIndex() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        client.newIndex("test1");
        client.close();
    }

    @Test
    void testMapping() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(5))
                .build();
        XContentBuilder builder = JsonXContent.contentBuilder()
                .startObject()
                .startObject("doc")
                .startObject("properties")
                .startObject("location")
                .field("type", "geo_point")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client.newIndex("test2", Settings.EMPTY, builder.string());
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices("test2");
        GetMappingsResponse getMappingsResponse =
                client.getClient().execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
        logger.info("mappings={}", getMappingsResponse.getMappings());
        assertTrue(getMappingsResponse.getMappings().get("test2").containsKey("doc"));
        client.close();
    }

    @Test
    void testSingleDoc() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(30))
                .build();
        try {
            client.newIndex("test3");
            client.index("test3", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(1, client.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.close();
        }
    }

    @Test
    void testRandomDocs() throws Exception {
        long numactions = ACTIONS;
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test4");
            for (int i = 0; i < ACTIONS; i++) {
                client.index("test4", null, false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(60L, TimeUnit.SECONDS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(numactions, client.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.refreshIndex("test4");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setIndices("test4")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0);
            assertEquals(numactions,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.close();
        }
    }

    @Test
    void testThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        long maxActionsPerRequest = MAX_ACTIONS_PER_REQUEST;
        final long actions = ACTIONS;
        logger.info("NodeClient max={} maxactions={} maxloop={}", maxthreads, maxActionsPerRequest, actions);
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_CONCURRENT_REQUESTS.name(), maxthreads)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), maxActionsPerRequest)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test5")
                    .startBulk("test5", -1, 1000);
            ThreadPoolExecutor pool = EsExecutors.newFixed("nodeclient-test", maxthreads, 30,
                    EsExecutors.daemonThreadFactory("nodeclient-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(() -> {
                    for (int i1 = 0; i1 < actions; i1++) {
                        client.index("test5", null, false,
                                "{ \"name\" : \"" + helper.randomString(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            logger.info("waiting for latch...");
            if (latch.await(60, TimeUnit.SECONDS)) {
                logger.info("flush...");
                client.flush();
                client.waitForResponses(60L, TimeUnit.SECONDS);
                logger.info("pool shutdown...");
                pool.shutdown();
                logger.info("pool is shut down");
            } else {
                logger.warn("latch timeout");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test5", 60L, TimeUnit.SECONDS);
            assertEquals(maxthreads * actions, client.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
            client.refreshIndex("test5");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setIndices("test5")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0);
            assertEquals(maxthreads * actions,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.close();
        }
    }
}
