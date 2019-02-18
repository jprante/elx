package org.xbib.elx.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class ExtendedTransportClientTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ExtendedTransportClientTest.class.getSimpleName());

    private static final Long MAX_ACTIONS_PER_REQUEST = 1000L;

    private static final Long ACTIONS = 1234L;

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
    public void testBulkClient() throws Exception {
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        client.newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        try {
            client.deleteIndex("test")
                    .newIndex("test")
                    .deleteIndex("test");
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

    @Test
    public void testSingleDocBulkClient() throws Exception {
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test");
            client.index("test", "test", "1", true, "{ \"name\" : \"Hello World\"}");
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
    public void testRandomDocsBulkClient() throws Exception {
        long numactions = ACTIONS;
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
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
        } catch (InterruptedException e) {
            // ignore
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(numactions, client.getBulkMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocsBulkClient() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        long maxactions = MAX_ACTIONS_PER_REQUEST;
        final long maxloop = ACTIONS;

        Settings settingsForIndex = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();

        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), maxactions)
                .put(Parameters.FLUSH_INTERVAL.name(), TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test", settingsForIndex, null)
                    .startBulk("test", -1, 1000);
            ThreadPoolExecutor pool = EsExecutors.newFixed("bulkclient-test", maxthreads, 30,
                            EsExecutors.daemonThreadFactory("bulkclient-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(() -> {
                    for (int i1 = 0; i1 < maxloop; i1++) {
                        client.index("test", "test", null, false,  "{ \"name\" : \"" + randomString(32) + "\"}");
                    }
                    latch.countDown();
                });
            }
            logger.info("waiting for latch...");
            if (latch.await(60, TimeUnit.SECONDS)) {
                logger.info("flush ...");
                client.flushIngest();
                client.waitForResponses("30s");
                logger.info("pool to be shut down ...");
                pool.shutdown();
                logger.info("poot shut down");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.stopBulk("test");
            assertEquals(maxthreads * maxloop, client.getBulkMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    // to avoid NPE at org.elasticsearch.action.search.SearchRequest.writeTo(SearchRequest.java:580)
                    .setIndices("_all")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0);
            assertEquals(maxthreads * maxloop,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            client.shutdown();
        }
    }
}
