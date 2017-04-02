package org.xbib.elasticsearch.extras.client.transport;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestBase;
import org.xbib.elasticsearch.extras.client.ClientBuilder;
import org.xbib.elasticsearch.extras.client.SimpleBulkControl;
import org.xbib.elasticsearch.extras.client.SimpleBulkMetric;

/**
 *
 */
public class BulkTransportDuplicateIDTest extends NodeTestBase {

    private static final Logger logger = LogManager.getLogger(BulkTransportDuplicateIDTest.class.getName());

    private static final long MAX_ACTIONS = 100L;

    private static final long NUM_ACTIONS = 12345L;

    @Test
    public void testDuplicateDocIDs() throws Exception {
        final BulkTransportClient client = ClientBuilder.builder()
                .put(getClientSettings())
                .put(ClientBuilder.MAX_CONCURRENT_REQUESTS, 2) // avoid EsRejectedExecutionException
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkTransportClient();
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
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
